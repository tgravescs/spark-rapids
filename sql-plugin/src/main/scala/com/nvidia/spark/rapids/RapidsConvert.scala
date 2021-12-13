/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryExpression, ComplexTypeMergingExpression, Expression, QuaternaryExpression, String2TrimExpression, TernaryExpression, UnaryExpression, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.rapids.{CpuToGpuAggregateBufferConverter, GpuToCpuAggregateBufferConverter}
import org.apache.spark.sql.types.DataType

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
abstract class RapidsConvert[INPUT <: BASE, BASE, OUTPUT <: BASE](
    val meta: RapidsMeta[INPUT, BASE, OUTPUT],
    rule: DataFromReplacementRule) {

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(): OUTPUT

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(): BASE = meta.wrapped

}

/**
 * Base class for metadata around `Partitioning`.
 */
abstract class PartConvert[INPUT <: Partitioning](
    partMeta: PartMeta[_],
    rule: DataFromReplacementRule)
  extends RapidsConvert[INPUT, Partitioning, GpuPartitioning](partMeta, rule) {

  override def convertToGpu(
      meta: PartMeta[INPUT]): GpuPartitioning = super.convertToGpu(partMeta)

}

/**
 * Metadata for Partitioning with no rule found
 */
final class RuleNotFoundPartConvert[INPUT <: Partitioning](
    partMeta: RuleNotFoundPartMeta[_])
  extends PartConvert[INPUT](partMeta, new NoRuleDataFromReplacementRule) {

  override def convertToGpu(): GpuPartitioning =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `Scan`.
 */
abstract class ScanConvert[INPUT <: Scan](
    scanMeta: ScanMeta[_],
    rule: DataFromReplacementRule)
  extends RapidsConvert[INPUT, Scan, Scan](scanMeta, rule) {
  }

/**
 * Metadata for `Scan` with no rule found
 */
final class RuleNotFoundScanConvert[INPUT <: Scan](
    scanMeta: RuleNotFoundScanMeta[_])
  extends ScanConvert[INPUT](scanMeta, new NoRuleDataFromReplacementRule) {

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
    rule: DataFromReplacementRule)
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
    extends DataWritingCommandMeta[INPUT](cmd, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU accelerated version of command ${cmd.getClass} could be found")
  }

  override def convertToGpu(): GpuDataWritingCommand =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `SparkPlan`.
 */
abstract class SparkPlanMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, SparkPlan, GpuExec](plan, conf, parent, rule) {

  def tagForExplain(): Unit = {
    if (!canThisBeReplaced) {
      childExprs.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childParts.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childScans.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childDataWriteCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    }
    if (shouldThisBeRemoved) {
      childExprs.foreach(_.recursiveSparkPlanRemoved())
      childParts.foreach(_.recursiveSparkPlanRemoved())
      childScans.foreach(_.recursiveSparkPlanRemoved())
      childDataWriteCmds.foreach(_.recursiveSparkPlanRemoved())
    }
    childPlans.foreach(_.tagForExplain())
  }

  def requireAstForGpuOn(exprMeta: BaseExprMeta[_]): Unit = {
    // willNotWorkOnGpu does not deduplicate reasons. Most of the time that is fine
    // but here we want to avoid adding the reason twice, because this method can be
    // called multiple times, and also the reason can automatically be added in if
    // a child expression would not work in the non-AST case either.
    // So only add it if canExprTreeBeReplaced changed after requiring that the
    // given expression is AST-able.
    val previousExprReplaceVal = canExprTreeBeReplaced
    exprMeta.requireAstForGpu()
    val newExprReplaceVal = canExprTreeBeReplaced
    if (previousExprReplaceVal != newExprReplaceVal &&
        !newExprReplaceVal) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }
  }

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] =
    plan.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] =
    plan.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] = Map.empty

  var cpuCost: Double = 0
  var gpuCost: Double = 0
  var estimatedOutputRows: Option[BigInt] = None

  override def convertToCpu(): SparkPlan = {
    wrapped.withNewChildren(childPlans.map(_.convertIfNeeded()))
  }

  def getReasonsNotToReplaceEntirePlan: Seq[String] = {
    val childReasons = childPlans.flatMap(_.getReasonsNotToReplaceEntirePlan)
    entirePlanExcludedReasons ++ childReasons
  }

  // For adaptive execution we have to ensure we mark everything properly
  // the first time through and that has to match what happens when AQE
  // splits things up and does the subquery analysis at the shuffle boundaries.
  // If the AQE subquery analysis changes the plan from what is originally
  // marked we can end up with mismatches like happened in:
  // https://github.com/NVIDIA/spark-rapids/issues/1423
  // AQE splits subqueries at shuffle boundaries which means that it only
  // sees the children at that point. So in our fix up exchange we only
  // look at the children and mark is at will not work on GPU if the
  // child can't be replaced.
  private def fixUpExchangeOverhead(): Unit = {
    childPlans.foreach(_.fixUpExchangeOverhead())
    if (wrapped.isInstanceOf[ShuffleExchangeExec] &&
      !childPlans.exists(_.canThisBeReplaced) &&
        (plan.conf.adaptiveExecutionEnabled ||
        !parent.exists(_.canThisBeReplaced))) {

      willNotWorkOnGpu("Columnar exchange without columnar children is inefficient")

      childPlans.head.wrapped
          .getTagValue(GpuOverrides.preRowToColProjection).foreach { r2c =>
        wrapped.setTagValue(GpuOverrides.preRowToColProjection, r2c)
      }
    }
  }

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
      childPlans.head.convertIfNeeded()
    } else {
      if (canThisBeReplaced) {
        convertToGpu()
      } else {
        convertToCpu()
      }
    }
  }
}


/**
 * Base class for metadata around `Expression`.
 */
abstract class BaseExprConvert[INPUT <: Expression](
    exprMeta: BaseExprMeta[_],
    rule: DataFromReplacementRule)
  extends RapidsConvert[INPUT, Expression, Expression](exprMeta[INPUT, Expression, Expression], rule) {
}

abstract class ExprConvert[INPUT <: Expression](
    exprMeta: ExprMeta[_],
    rule: DataFromReplacementRule)
    extends BaseExprConvert[INPUT](exprMeta, rule) {

  override def convertToGpu(): GpuExpression
}

/**
 * Base class for metadata around `UnaryExpression`.
 */
abstract class UnaryExprConvert[INPUT <: UnaryExpression](
    exprMeta: UnaryExprMeta[_],
    rule: DataFromReplacementRule)
  extends ExprConvert[INPUT](exprMeta, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(exprMeta.childExprs.head.convertToGpu())

  def convertToGpu(child: Expression): GpuExpression
}

/** Base metadata class for unary expressions that support conversion to AST as well */
abstract class UnaryAstExprConvert[INPUT <: UnaryExpression](
    expr: UnaryAstExprMeta[_],
    rule: DataFromReplacementRule)
    extends UnaryExprConvert[INPUT](expr, rule) {
}

/**
 * Base class for metadata around `AggregateFunction`.
 */
abstract class AggExprConvert[INPUT <: AggregateFunction](
    val exprMeta: AggExprMeta[_],
    rule: DataFromReplacementRule)
  extends ExprConvert[INPUT](exprMeta, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(exprMeta.childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Base class for metadata around `ImperativeAggregate`.
 */
abstract class ImperativeAggExprConvert[INPUT <: ImperativeAggregate](
    exprMeta: ImperativeAggExprMeta[_],
    rule: DataFromReplacementRule)
  extends AggExprConvert[INPUT](exprMeta, rule) {

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Base class for metadata around `TypedImperativeAggregate`.
 */
abstract class TypedImperativeAggExprConvert[INPUT <: TypedImperativeAggregate[_]](
    expr: TypedImperativeAggExprMeta[_],
    rule: DataFromReplacementRule)
    extends ImperativeAggExprConvert[INPUT](expr, rule) {

}

/**
 * Base class for metadata around `BinaryExpression`.
 */
abstract class BinaryExprConvert[INPUT <: BinaryExpression](
    exprMeta: BinaryExprMeta[_],
    rule: DataFromReplacementRule)
  extends ExprConvert[INPUT](exprMeta, rule) {

  override final def convertToGpu(): GpuExpression = {
    val Seq(lhs, rhs) = exprMeta.childExprs.map(_.convertToGpu())
    convertToGpu(lhs, rhs)
  }

  def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression
}

/** Base metadata class for binary expressions that support conversion to AST */
abstract class BinaryAstExprConvert[INPUT <: BinaryExpression](
    exprMeta: BinaryAstExprMeta[_],
    rule: DataFromReplacementConvert[INPUT](exprMeta, rule) {
}

/**
 * Base class for metadata around `TernaryExpression`.
 */
abstract class TernaryExprConvert[INPUT <: TernaryExpression](
    exprMeta: TernaryExprMeta[_],
    rule: DataFromReplacementRule)
  extends ExprConvert[INPUT](exprMeta, rule) {

  override final def convertToGpu(): GpuExpression = {
    val Seq(child0, child1, child2) = exprMeta.childExprs.map(_.convertToGpu())
    convertToGpu(child0, child1, child2)
  }

  def convertToGpu(val0: Expression, val1: Expression,
                   val2: Expression): GpuExpression
}

/**
 * Base class for metadata around `QuaternaryExpression`.
 */
abstract class QuaternaryExprConvert[INPUT <: QuaternaryExpression](
    exprMeta: QuaternaryExprMeta[_],
    rule: DataFromReplacementRule)
  extends ExprConvert[INPUT](exprMeta, rule) {

  override final def convertToGpu(): GpuExpression = {
    val Seq(child0, child1, child2, child3) = exprMeta.childExprs.map(_.convertToGpu())
    convertToGpu(child0, child1, child2, child3)
  }

  def convertToGpu(val0: Expression, val1: Expression,
    val2: Expression, val3: Expression): GpuExpression
}

abstract class String2TrimExpressionConvert[INPUT <: String2TrimExpression](
    exprMeta: String2TrimExpressionMeta[_],
    rule: DataFromReplacementRule)
    extends ExprConvert[INPUT](exprMeta, rule) {

  override final def convertToGpu(): GpuExpression = {
    val gpuCol :: gpuTrimParam = exprMeta.childExprs.map(_.convertToGpu())
    convertToGpu(gpuCol, gpuTrimParam.headOption)
  }

  def convertToGpu(column: Expression, target: Option[Expression] = None): GpuExpression
}

/**
 * Base class for metadata around `ComplexTypeMergingExpression`.
 */
abstract class ComplexTypeMergingExprConvert[INPUT <: ComplexTypeMergingExpression](
    exprMeta: ComplexTypeMergingExprMeta[_],
    rule: DataFromReplacementRule)
  extends ExprConvert[INPUT](exprMeta, rule) {
  override final def convertToGpu(): GpuExpression =
    convertToGpu(exprMeta.childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Metadata for `Expression` with no rule found
 */
final class RuleNotFoundExprConvert[INPUT <: Expression](
    exprMeta: RuleNotFoundExprMeta[_],
    parent: Option[RapidsMeta[_, _, _]])
  extends ExprConvert[INPUT](exprMeta, new NoRuleDataFromReplacementRule) {

  override def convertToGpu(): GpuExpression =
    throw new IllegalStateException("Cannot be converted to GPU")
}
