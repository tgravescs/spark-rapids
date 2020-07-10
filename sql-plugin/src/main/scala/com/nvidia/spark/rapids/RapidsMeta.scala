/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.time.ZoneId

import scala.collection.mutable
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ComplexTypeMergingExpression, Expression, NamedExpression, SortOrder, String2TrimExpression, TernaryExpression, UnaryExpression, WindowFrame}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.unsafe.types._
import org.apache.spark.sql.types._
import org.apache.spark.internal.Logging

trait ConfKeysAndIncompat {
  val operationName: String
  def incompatDoc: Option[String] = None
  def disabledMsg: Option[String] = None

  def confKey: String
}

object RapidsMeta {

  val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()

  def areAllSupportedTypes(types: DataType*): Boolean = types.forall(isSupportedType)

  def isSupportedType(dataType: DataType): Boolean = dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case DateType => true
      case TimestampType => ZoneId.systemDefault().normalized() == RapidsMeta.UTC_TIMEZONE_ID
      case StringType => true
      case _ => false
  }


  @scala.annotation.tailrec
  def extractLit(exp: Expression): Option[Literal] = exp match {
    case l: Literal => Some(l)
    case a: Alias => extractLit(a.child)
    case _ => None
  }

  def isOfType(l: Option[Literal], t: DataType): Boolean = l.exists(_.dataType == t)

  def isStringLit(exp: Expression): Boolean =
    isOfType(extractLit(exp), StringType)

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.exists(isStringLit)

  def extractStringLit(exp: Expression): Option[String] = extractLit(exp) match {
    case Some(Literal(v: UTF8String, StringType)) =>
      val s = if (v == null) null else v.toString
      Some(s)
    case _ => None
  }

}


/**
 * A version of ConfKeysAndIncompat that is used when no replacement rule can be found.
 */
final class NoRuleConfKeysAndIncompat extends ConfKeysAndIncompat {
  override val operationName: String = "NOT_FOUND"

  override def confKey = "NOT_FOUND"
}

/**
 * Holds metadata about a stage in the physical plan that is separate from the plan itself.
 * This is helpful in deciding when to replace part of the plan with a GPU enabled version.
 *
 * @param wrapped what we are wrapping
 * @param conf the config
 * @param parent the parent of this node, if there is one.
 * @param rule holds information related to the config for this object, typically this is the rule
 *          used to wrap the stage.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam OUTPUT when converting to a GPU enabled version of the plan, the generic base
 *                    type for all GPU enabled versions.
 */
abstract class RapidsMeta[INPUT <: BASE, BASE, OUTPUT <: BASE](
    val wrapped: INPUT,
    val conf: RapidsConf,
    val parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat) {

  /**
   * The wrapped plans that should be examined
   */
  val childPlans: Seq[SparkPlanMeta[_]]

  /**
   * The wrapped expressions that should be examined
   */
  val childExprs: Seq[BaseExprMeta[_]]

  /**
   * The wrapped scans that should be examined
   */
  val childScans: Seq[ScanMeta[_]]

  /**
   * The wrapped partitioning that should be examined
   */
  val childParts: Seq[PartMeta[_]]

  /**
   * The wrapped data writing commands that should be examined
   */
  val childDataWriteCmds: Seq[DataWritingCommandMeta[_]]

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(): OUTPUT

  /**
   * Check if all the types are supported in this Meta
   */
  def areAllSupportedTypes(types: DataType*): Boolean = {
    RapidsMeta.areAllSupportedTypes(types: _*)
  }

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(): BASE = wrapped

  private var cannotBeReplacedReasons: Option[mutable.Set[String]] = None

  private var shouldBeRemovedReasons: Option[mutable.Set[String]] = None

  /**
   * Call this to indicate that this should not be replaced with a GPU enabled version
   * @param because why it should not be replaced.
   */
  final def willNotWorkOnGpu(because: String): Unit =
    cannotBeReplacedReasons.get.add(because)

  final def shouldBeRemoved(because: String): Unit =
    shouldBeRemovedReasons.get.add(because)

  /**
   * Returns true if this node should be removed.
   */
  final def shouldThisBeRemoved: Boolean = shouldBeRemovedReasons.exists(_.nonEmpty)

  /**
   * Returns true iff this could be replaced.
   */
  final def canThisBeReplaced: Boolean = cannotBeReplacedReasons.exists(_.isEmpty)

  /**
   * Returns true iff all of the expressions and their children could be replaced.
   */
  def canExprTreeBeReplaced: Boolean = childExprs.forall(_.canExprTreeBeReplaced)

  /**
   * Returns true iff all of the scans can be replaced.
   */
  def canScansBeReplaced: Boolean = childScans.forall(_.canThisBeReplaced)

  /**
   * Returns true iff all of the partitioning can be replaced.
   */
  def canPartsBeReplaced: Boolean = childParts.forall(_.canThisBeReplaced)

  /**
   * Returns true iff all of the data writing commands can be replaced.
   */
  def canDataWriteCmdsBeReplaced: Boolean = childDataWriteCmds.forall(_.canThisBeReplaced)

  def confKey: String = rule.confKey
  final val operationName: String = rule.operationName
  final val incompatDoc: Option[String] = rule.incompatDoc
  def isIncompat: Boolean = incompatDoc.isDefined
  final val disabledMsg: Option[String] = rule.disabledMsg
  def isDisabledByDefault: Boolean = disabledMsg.isDefined

  def initReasons(): Unit = {
    cannotBeReplacedReasons = Some(mutable.Set[String]())
    shouldBeRemovedReasons = Some(mutable.Set[String]())
  }

  /**
   * Tag all of the children to see if they are GPU compatible first.
   * Do basic common verification for the operators, and then call
   * [[tagSelfForGpu]]
   */
  final def tagForGpu(): Unit = {
    childScans.foreach(_.tagForGpu())
    childParts.foreach(_.tagForGpu())
    childExprs.foreach(_.tagForGpu())
    childDataWriteCmds.foreach(_.tagForGpu())
    childPlans.foreach(_.tagForGpu())

    initReasons()

    if (!conf.isOperatorEnabled(confKey, isIncompat, isDisabledByDefault)) {
      if (isIncompat && !conf.isIncompatEnabled) {
        willNotWorkOnGpu(s"the GPU version of ${wrapped.getClass.getSimpleName}" +
            s" is not 100% compatible with the Spark version. ${incompatDoc.get}. To enable this" +
            s" $operationName despite the incompatibilities please set the config" +
            s" $confKey to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
            s" to enable all incompatible ops")
      } else if (isDisabledByDefault) {
        willNotWorkOnGpu(s"the $operationName ${wrapped.getClass.getSimpleName} has" +
            s" been disabled, and is disabled by default because ${disabledMsg.get}. Set $confKey" +
            s" to true if you wish to enable it")
      } else {
        willNotWorkOnGpu(s"the $operationName ${wrapped.getClass.getSimpleName} has" +
            s" been disabled. Set $confKey to true if you wish to enable it")
      }
    }

    tagSelfForGpu()
  }

  /**
   * Do any extra checks and tag yourself if you are compatible or not.  Be aware that this may
   * already have been marked as incompatible for a number of reasons.
   *
   * All of your children should have already been tagged so if there are situations where you
   * may need to disqualify your children for various reasons you may do it here too.
   */
  def tagSelfForGpu(): Unit

  private def indent(append: StringBuilder, depth: Int): Unit =
    append.append("  " * depth)

  private def willWorkOnGpuInfo: String = cannotBeReplacedReasons match {
    case None => "NOT EVALUATED FOR GPU YET"
    case Some(v) if v.isEmpty => "could run on GPU"
    case Some(v) =>
      val reasons = v mkString "; "
      s"cannot run on GPU because ${reasons}"
  }

  private def willBeRemovedInfo: String = shouldBeRemovedReasons match {
    case None => ""
    case Some(v) if v.isEmpty => ""
    case Some(v) =>
      val reasons = v mkString "; "
      s" but is going to be removed because ${reasons}"
  }

  /**
   * When converting this to a string should we include the string representation of what this
   * wraps too?  This is off by default.
   */
  protected val printWrapped = false

  /**
   * Create a string representation of this in append.
   * @param strBuilder where to place the string representation.
   * @param depth how far down the tree this is.
   * @param all should all the data be printed or just what does not work on the GPU?
   */
  protected def print(strBuilder: StringBuilder, depth: Int, all: Boolean): Unit = {
    if (all || !canThisBeReplaced) {
      indent(strBuilder, depth)
      strBuilder.append(if (canThisBeReplaced) "*" else "!")

      strBuilder.append(operationName)
        .append(" <")
        .append(wrapped.getClass.getSimpleName)
        .append("> ")

      if (printWrapped) {
        strBuilder.append(wrapped)
          .append(" ")
      }

      strBuilder.append(willWorkOnGpuInfo).
        append(willBeRemovedInfo).
        append("\n")
    }
    printChildren(strBuilder, depth, all)
  }

  private final def printChildren(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    childScans.foreach(_.print(append, depth + 1, all))
    childParts.foreach(_.print(append, depth + 1, all))
    childExprs.foreach(_.print(append, depth + 1, all))
    childDataWriteCmds.foreach(_.print(append, depth + 1, all))
    childPlans.foreach(_.print(append, depth + 1, all))
  }

  def explain(all: Boolean): String = {
    val appender = new StringBuilder()
    print(appender, 0, all)
    appender.toString()
  }

  override def toString: String = {
    explain(true)
  }
}

/**
 * Base class for metadata around `Partitioning`.
 */
abstract class PartMeta[INPUT <: Partitioning](part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, Partitioning, GpuPartitioning](part, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override final def tagSelfForGpu(): Unit = {
    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }
    tagPartForGpu()
  }

  def tagPartForGpu(): Unit = {}
}

/**
 * Metadata for Partitioning]] with no rule found
 */
final class RuleNotFoundPartMeta[INPUT <: Partitioning](
    part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends PartMeta[INPUT](part, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagPartForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of partitioning ${part.getClass} could be found")
  }

  override def convertToGpu(): GpuPartitioning =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `Scan`.
 */
abstract class ScanMeta[INPUT <: Scan](scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, Scan, Scan](scan, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override def tagSelfForGpu(): Unit = {}
}

/**
 * Metadata for `Scan` with no rule found
 */
final class RuleNotFoundScanMeta[INPUT <: Scan](
    scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ScanMeta[INPUT](scan, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of scan ${scan.getClass} could be found")
  }

  override def convertToGpu(): Scan =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `DataWritingCommand`.
 */
abstract class DataWritingCommandMeta[INPUT <: DataWritingCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
    extends RapidsMeta[INPUT, DataWritingCommand, GpuDataWritingCommand](cmd, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override def tagSelfForGpu(): Unit = {}
}

/**
 * Metadata for `DataWritingCommand` with no rule found
 */
final class RuleNotFoundDataWritingCommandMeta[INPUT <: DataWritingCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
    extends DataWritingCommandMeta[INPUT](cmd, conf, parent, new NoRuleConfKeysAndIncompat) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of command ${cmd.getClass} could be found")
  }

  override def convertToGpu(): GpuDataWritingCommand =
    throw new IllegalStateException("Cannot be converted to GPU")
}

sealed abstract class GpuBuildSideT

case object GpuBuildRightT extends GpuBuildSideT

case object GpuBuildLeftT extends GpuBuildSideT

/**
 * Base class for metadata around `SparkPlan`.
 */
abstract class SparkPlanMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, SparkPlan, GpuExec](plan, conf, parent, rule) with Logging {

  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override def convertToCpu(): SparkPlan = {
    logWarning("child plans are: " + childPlans)
    val converted = childPlans.map(_.convertIfNeeded())
    logWarning("converted child plans are: " + converted)
    wrapped.withNewChildren(childPlans.map(_.convertIfNeeded()))
  }


 private def getBuildSide(join: BroadcastHashJoinExec): GpuBuildSideT = {
    join.buildSide match {
      case e: join.buildSide.type if e.toString.contains("BuildRight") => {
        GpuBuildRightT
      }
      case l: join.buildSide.type if l.toString.contains("BuildLeft") => {
        GpuBuildLeftT
      }
      case _ => throw new Exception("unknown buildSide Type")
    }
  }

  private def findShuffleExchanges(): Seq[SparkPlanMeta[ShuffleExchangeExec]] = wrapped match {
    case _: ShuffleExchangeExec =>
      this.asInstanceOf[SparkPlanMeta[ShuffleExchangeExec]] :: Nil
    case bkj: BroadcastHashJoinExec => getBuildSide(bkj) match {
      case GpuBuildLeftT => childPlans(1).findShuffleExchanges()
      case GpuBuildRightT => childPlans(0).findShuffleExchanges()
    }
    case _ => childPlans.flatMap(_.findShuffleExchanges())
  }

  private def makeShuffleConsistent(): Unit = {
    val exchanges = findShuffleExchanges()
    if (!exchanges.forall(_.canThisBeReplaced)) {
      exchanges.foreach(_.willNotWorkOnGpu("other exchanges that feed the same join are" +
        " on the CPU and GPU hashing is not consistent with the CPU version"))
    }
  }

  private def fixUpJoinConsistencyIfNeeded(): Unit = {
    childPlans.foreach(_.fixUpJoinConsistencyIfNeeded())
    wrapped match {
      case _: ShuffledHashJoinExec => makeShuffleConsistent()
      case _: SortMergeJoinExec => makeShuffleConsistent()
      case _ => ()
    }
  }

  private def fixUpExchangeOverhead(): Unit = {
    childPlans.foreach(_.fixUpExchangeOverhead())
    if (wrapped.isInstanceOf[ShuffleExchangeExec] &&
      (parent.filter(_.canThisBeReplaced).isEmpty &&
        childPlans.filter(_.canThisBeReplaced).isEmpty)) {
      willNotWorkOnGpu("Columnar exchange without columnar children is inefficient")
    }
  }

  /**
   * Run rules that happen for the entire tree after it has been tagged initially.
   */
  def runAfterTagRules(): Unit = {
    // In the first pass tagSelfForGpu will deal with each operator individually.
    // Children will be tagged first and then their parents will be tagged.  This gives
    // flexibility when tagging yourself to look at your children and disable yourself if your
    // children are not all on the GPU.  In some cases we need to be able to disable our
    // children too, or in this case run a rule that will disable operations when looking at
    // more of the tree.  These exceptions should be documented here.  We need to take special care
    // that we take into account all side-effects of these changes, because we are **not**
    // re-triggering the rules associated with parents, grandparents, etc.  If things get too
    // complicated we may need to update this to have something with triggers, but then we would
    // have to be very careful to avoid loops in the rules.

    // RULES:
    // 1) BroadcastHashJoin can disable the Broadcast directly feeding it, if the join itself cannot
    // be translated for some reason.  This is okay because it is the joins immediate parent, so
    // it can keep everything consistent.
    // 2) For ShuffledHashJoin and SortMergeJoin we need to verify that all of the exchanges
    // feeding them are either all on the GPU or all on the CPU, because the hashing is not
    // consistent between the two implementations. This is okay because it is only impacting
    // shuffled exchanges. So broadcast exchanges are not impacted which could have an impact on
    // BroadcastHashJoin, and shuffled exchanges are not used to disable anything downstream.
    // 3) If a shuffled exchange is not columnar on at least one side don't do it.  This must happen
    // before the join consistency or we risk running into issues with disabling one exchange that
    // would make a join inconsistent
    fixUpExchangeOverhead()
    fixUpJoinConsistencyIfNeeded()
  }

  override final def tagSelfForGpu(): Unit = {
    if (!areAllSupportedTypes(plan.output.map(_.dataType) :_*)) {
      val unsupported = plan.output.map(_.dataType).filter(!areAllSupportedTypes(_)).toSet
      willNotWorkOnGpu(s"unsupported data types in output: ${unsupported.mkString(", ")}")
    }
    if (!areAllSupportedTypes(plan.children.flatMap(_.output.map(_.dataType)) :_*)) {
      val unsupported = plan.children.flatMap(_.output.map(_.dataType))
        .filter(!areAllSupportedTypes(_)).toSet
      willNotWorkOnGpu(s"unsupported data types in input: ${unsupported.mkString(", ")}")
    }

    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }

    if (!canScansBeReplaced) {
      willNotWorkOnGpu("not all scans can be replaced")
    }

    if (!canPartsBeReplaced) {
      willNotWorkOnGpu("not all partitioning can be replaced")
    }

    if (!canDataWriteCmdsBeReplaced) {
      willNotWorkOnGpu("not all data writing commands can be replaced")
    }

    tagPlanForGpu()
  }

  /**
   * Called to verify that this plan will work on the GPU. Generic checks will have already been
   * done. In general this method should only tag this operator as bad.  If it needs to tag
   * one of its children please take special care to update the comment inside
   * `tagSelfForGpu` so we don't end up with something that could be cyclical.
   */
  def tagPlanForGpu(): Unit = {}

  /**
   * If this is enabled to be converted to a GPU version convert it and return the result, else
   * do what is needed to possibly convert the rest of the plan.
   */
  final def convertIfNeeded(): SparkPlan = {
    if (shouldThisBeRemoved) {
      if (childPlans.isEmpty) {
        throw new IllegalStateException("can't remove when plan has no children")
      } else if (childPlans.size > 1) {
        throw new IllegalStateException("can't remove when plan has more than 1 child")
      }
      childPlans(0).convertIfNeeded()
    } else {
      if (canThisBeReplaced) {
        convertToGpu()
      } else {
        convertToCpu()
      }
    }
  }
}

abstract class GpuHashJoinBaseMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

    val leftKeys: Seq[BaseExprMeta[_]]
    val rightKeys: Seq[BaseExprMeta[_]]
    val condition: Option[BaseExprMeta[_]]
}


abstract class WindowExecBaseMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {
  val windowExpressions: Seq[BaseExprMeta[NamedExpression]]
}



/**
 * Base class for metadata around `Expression`.
 */
abstract class BaseExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends RapidsMeta[INPUT, Expression, Expression](expr, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override val printWrapped: Boolean = true

  val ignoreUnsetDataTypes = false

  override def canExprTreeBeReplaced: Boolean =
    canThisBeReplaced && super.canExprTreeBeReplaced

  final override def tagSelfForGpu(): Unit = {
    try {
      if (!areAllSupportedTypes(expr.dataType)) {
        willNotWorkOnGpu(s"expression ${expr.getClass.getSimpleName} ${expr} " +
          s"produces an unsupported type ${expr.dataType}")
      }
    }
    catch {
      case _ : java.lang.UnsupportedOperationException =>
        if (!ignoreUnsetDataTypes) {
          willNotWorkOnGpu(s"expression ${expr.getClass.getSimpleName} ${expr} " +
            s" does not have a corresponding dataType.")
        }
    }
    tagExprForGpu()
  }

  /**
   * Called to verify that this expression will work on the GPU. For most expressions without
   * extra checks all of the checks should have already been done.
   */
  def tagExprForGpu(): Unit = {}
}

abstract class ExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
    extends BaseExprMeta[INPUT](expr, conf, parent, rule) {

  override def convertToGpu(): GpuExpression
}

abstract class WindowExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  val partitionSpec: Seq[BaseExprMeta[Expression]]
  val orderSpec: Seq[BaseExprMeta[SortOrder]]
  val windowFrame: BaseExprMeta[WindowFrame]
}


/**
 * Base class for metadata around `UnaryExpression`.
 */
abstract class UnaryExprMeta[INPUT <: UnaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) with Logging{

  override final def convertToGpu(): GpuExpression = {
    logWarning("convert to gpu for: " + expr + " exprs: " + childExprs)  
    convertToGpu(childExprs(0).convertToGpu())
  }

  def convertToGpu(child: Expression): GpuExpression
}

/**
 * Base class for metadata around `AggregateFunction`.
 */
abstract class AggExprMeta[INPUT <: AggregateFunction](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs(0).convertToGpu())

  def convertToGpu(child: Expression): GpuExpression
}

/**
 * Base class for metadata around `BinaryExpression`.
 */
abstract class BinaryExprMeta[INPUT <: BinaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs(0).convertToGpu(), childExprs(1).convertToGpu())

  def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression
}

/**
 * Base class for metadata around `TernaryExpression`.
 */
abstract class TernaryExprMeta[INPUT <: TernaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs(0).convertToGpu(), childExprs(1).convertToGpu(),
                 childExprs(2).convertToGpu())

  def convertToGpu(val0: Expression, val1: Expression,
                   val2: Expression): GpuExpression
}

abstract class String2TrimExpressionMeta[INPUT <: String2TrimExpression](
    expr: INPUT,
    trimStr: Option[Expression],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
    extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    if (trimStr != None && !RapidsMeta.isStringLit(trimStr.get)) {
      willNotWorkOnGpu("only literal parameters supported for string literal trimStr parameter")
    }
  }

  override final def convertToGpu(): GpuExpression = {
    val trimParam = if (childExprs.size > 1) {
      Some(childExprs(1).convertToGpu())
    } else {
      None
    }
    convertToGpu(childExprs(0).convertToGpu(), trimParam)
  }

  def convertToGpu(column: Expression, target: Option[Expression] = None): GpuExpression
}

/**
 * Base class for metadata around `ComplexTypeMergingExpression`.
 */
abstract class ComplexTypeMergingExprMeta[INPUT <: ComplexTypeMergingExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {
  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

