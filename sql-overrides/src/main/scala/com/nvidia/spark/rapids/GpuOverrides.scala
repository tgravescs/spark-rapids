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

import scala.reflect.ClassTag

import java.time.ZoneId

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims._
import org.apache.spark.sql.rapids.execution._

import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, ShuffledHashJoinExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinMeta, GpuBroadcastMeta, GpuBroadcastNestedLoopJoinMeta}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Base class for all ReplacementRules
 * @param doWrap wraps a part of the plan in a [[RapidsMeta]] for further processing.
 * @param desc a description of what this part of the plan does.
 * @param tag metadata used to determine what INPUT is at runtime.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam WRAP_TYPE base class that should be returned by doWrap.
 */
abstract class ReplacementRule[INPUT <: BASE, BASE, WRAP_TYPE <: RapidsMeta[INPUT, BASE, _]](
    protected var doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => WRAP_TYPE,
    protected var desc: String,
    final val tag: ClassTag[INPUT]) extends ConfKeysAndIncompat {

  private var _incompatDoc: Option[String] = None
  private var _disabledDoc: Option[String] = None

  override def incompatDoc: Option[String] = _incompatDoc
  override def disabledMsg: Option[String] = _disabledDoc

  /**
   * Mark this expression as incompatible with the original Spark version
   * @param str a description of how it is incompatible.
   * @return this for chaining.
   */
  final def incompat(str: String) : this.type = {
    _incompatDoc = Some(str)
    this
  }

  /**
   * Mark this expression as disabled by default.
   * @param str a description of why it is disabled by default.
   * @return this for chaining.
   */
  final def disabledByDefault(str: String) : this.type = {
    _disabledDoc = Some(str)
    this
  }

  /**
   * Provide a function that will wrap a spark type in a [[RapidsMeta]] instance that is used for
   * conversion to a GPU version.
   * @param func the function
   * @return this for chaining.
   */
  final def wrap(func: (
      INPUT,
      RapidsConf,
      Option[RapidsMeta[_, _, _]],
      ConfKeysAndIncompat) => WRAP_TYPE): this.type = {
    doWrap = func
    this
  }

  /**
   * Set a description of what the operation does.
   * @param str the description.
   * @return this for chaining
   */
  final def desc(str: String): this.type = {
    this.desc = str
    this
  }

  private var confKeyCache: String = null
  protected val confKeyPart: String

  override def confKey: String = {
    if (confKeyCache == null) {
      confKeyCache = "spark.rapids.sql." + confKeyPart + "." + tag.runtimeClass.getSimpleName
    }
    confKeyCache
  }

  private def notes(): Option[String] = if (incompatDoc.isDefined) {
    Some(s"This is not 100% compatible with the Spark version because ${incompatDoc.get}")
  } else if (disabledMsg.isDefined) {
    Some(s"This is disabled by default because ${disabledMsg.get}")
  } else {
    None
  }

  def confHelp(asTable: Boolean = false): Unit = {
    val notesMsg = notes()
    if (asTable) {
      import ConfHelper.makeConfAnchor
      print(s"${makeConfAnchor(confKey)}|$desc|${notesMsg.isEmpty}|")
      if (notesMsg.isDefined) {
        print(s"${notesMsg.get}")
      } else {
        print("None")
      }
      println("|")
    } else {
      println(s"$confKey:")
      println(s"\tEnable (true) or disable (false) the $tag $operationName.")
      println(s"\t$desc")
      if (notesMsg.isDefined) {
        println(s"\t${notesMsg.get}")
      }
      println(s"\tdefault: ${notesMsg.isEmpty}")
      println()
    }
  }

  final def wrap(
      op: BASE,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      r: ConfKeysAndIncompat): WRAP_TYPE = {
    doWrap(op.asInstanceOf[INPUT], conf, parent, r)
  }

  def getClassFor: Class[_] = tag.runtimeClass
}

/**
 * Holds everything that is needed to replace an Expression with a GPU enabled version.
 */
class ExprRule[INPUT <: Expression](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => BaseExprMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Expression, BaseExprMeta[INPUT]](doWrap, desc, tag) {

  override val confKeyPart = "expression"
  override val operationName = "Expression"
}

/**
 * Holds everything that is needed to replace a `Scan` with a GPU enabled version.
 */
class ScanRule[INPUT <: Scan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => ScanMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Scan, ScanMeta[INPUT]](doWrap, desc, tag) {

  override val confKeyPart: String = "input"
  override val operationName: String = "Input"
}

/**
 * Metadata for `Expression` with no rule found
 */
final class RuleNotFoundExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ExprMeta[INPUT](expr, conf, parent, new NoRuleConfKeysAndIncompat) {

 override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagExprForGpu(): Unit =
    willNotWorkOnGpu(s"no GPU enabled version of expression ${expr.getClass} could be found")

  override def convertToGpu(): GpuExpression =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Metadata for `SparkPlan` with no rule found
 */
final class RuleNotFoundSparkPlanMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends SparkPlanMeta[INPUT](plan, conf, parent, new NoRuleConfKeysAndIncompat) {

  override val childPlans: Seq[SparkPlanMeta[_]] =
    plan.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] =
    plan.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit =
    willNotWorkOnGpu(s"no GPU enabled version of operator ${plan.getClass} could be found")

  override def convertToGpu(): GpuExec =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Holds everything that is needed to replace a `Partitioning` with a GPU enabled version.
 */
class PartRule[INPUT <: Partitioning](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => PartMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Partitioning, PartMeta[INPUT]](doWrap, desc, tag) {

  override val confKeyPart: String = "partitioning"
  override val operationName: String = "Partitioning"
}

/**
 * Holds everything that is needed to replace a `SparkPlan` with a GPU enabled version.
 */
class ExecRule[INPUT <: SparkPlan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => SparkPlanMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, SparkPlan, SparkPlanMeta[INPUT]](doWrap, desc, tag) {

  override val confKeyPart: String = "exec"
  override val operationName: String = "Exec"
}

/**
 * Holds everything that is needed to replace a `DataWritingCommand` with a
 * GPU enabled version.
 */
class DataWritingCommandRule[INPUT <: DataWritingCommand](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => DataWritingCommandMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
    extends ReplacementRule[INPUT, DataWritingCommand, DataWritingCommandMeta[INPUT]](
      doWrap, desc, tag) {

  override val confKeyPart: String = "output"
  override val operationName: String = "Output"
}

final class InsertIntoHadoopFsRelationCommandMeta(
    cmd: InsertIntoHadoopFsRelationCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
    extends DataWritingCommandMeta[InsertIntoHadoopFsRelationCommand](cmd, conf, parent, rule) {

  private var fileFormat: Option[ColumnarFileFormat] = None

  override def tagSelfForGpu(): Unit = {
    if (cmd.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }

     val spark = SparkSession.active

    fileFormat = cmd.fileFormat match {
      case _: CSVFileFormat =>
        willNotWorkOnGpu("CSV output is not supported")
        None
      case _: JsonFileFormat =>
        willNotWorkOnGpu("JSON output is not supported")
        None
      case _: OrcFileFormat =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.options)
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
      case _: TextFileFormat =>
        willNotWorkOnGpu("text output is not supported")
        None
      case f =>
        willNotWorkOnGpu(s"unknown file format: ${f.getClass.getCanonicalName}")
        None
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val format = fileFormat.getOrElse(
      throw new IllegalStateException("fileFormat missing, tagSelfForGpu not called?"))

    GpuInsertIntoHadoopFsRelationCommand(
      cmd.outputPath,
      cmd.staticPartitions,
      cmd.ifPartitionNotExists,
      cmd.partitionColumns,
      cmd.bucketSpec,
      format,
      cmd.options,
      cmd.query,
      cmd.mode,
      cmd.catalogTable,
      cmd.fileIndex,
      cmd.outputColumnNames)
  }
}

object GpuOverrides extends Logging {
  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping " +
    "keys (e.g. -0.0, and 0.0)"
  val CASE_MODIFICATION_INCOMPAT =
    "in some cases unicode characters change byte width when changing the case. The GPU string " +
    "conversion does not support these characters. For a full list of unsupported characters " +
    "see https://github.com/rapidsai/cudf/issues/3132"
  val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()
  // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
  private[this] lazy val regexList: Seq[String] = Seq("\\", "\u0000", "\\x", "\t", "\n", "\r",
    "\f", "\\a", "\\e", "\\cx", "[", "]", "^", "&", ".", "*", "\\d", "\\D", "\\h", "\\H", "\\s",
    "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B", "\\A", "\\G", "\\Z", "\\z", "\\R",
    "?", "|", "(", ")", "{", "}", "\\k", "\\Q", "\\E", ":", "!", "<=", ">")

  @scala.annotation.tailrec
  def extractLit(exp: Expression): Option[Literal] = exp match {
    case l: Literal => Some(l)
    case a: Alias => extractLit(a.child)
    case _ => None
  }

  def isOfType(l: Option[Literal], t: DataType): Boolean = l.exists(_.dataType == t)

  def isStringLit(exp: Expression): Boolean =
    isOfType(extractLit(exp), StringType)

  def extractStringLit(exp: Expression): Option[String] = extractLit(exp) match {
    case Some(Literal(v: UTF8String, StringType)) =>
      val s = if (v == null) null else v.toString
      Some(s)
    case _ => None
  }

  def isLit(exp: Expression): Boolean = extractLit(exp).isDefined

  def isNullLit(lit: Literal): Boolean = {
    lit.value == null
  }

  def isNullOrEmptyOrRegex(exp: Expression): Boolean = {
    val lit = extractLit(exp)
    if (!isOfType(lit, StringType)) {
      false
    } else if (isNullLit(lit.get)) {
      //isOfType check above ensures that this lit.get does not throw
      true
    } else {
      val strLit = lit.get.value.asInstanceOf[UTF8String].toString
      if (strLit.isEmpty) {
        true
      } else {
        regexList.exists(pattern => strLit.contains(pattern))
      }
    }
  }

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.exists(isStringLit)

  def expr[INPUT <: Expression](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => BaseExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExprRule[INPUT](doWrap, desc, tag)
  }

  def scan[INPUT <: Scan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => ScanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ScanRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ScanRule[INPUT](doWrap, desc, tag)
  }

  def part[INPUT <: Partitioning](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => PartMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): PartRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new PartRule[INPUT](doWrap, desc, tag)
  }

  def exec[INPUT <: SparkPlan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => SparkPlanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExecRule[INPUT](doWrap, desc, tag)
  }

  def dataWriteCmd[INPUT <: DataWritingCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
        => DataWritingCommandMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): DataWritingCommandRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new DataWritingCommandRule[INPUT](doWrap, desc, tag)
  }

  def wrapExpr[INPUT <: Expression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): BaseExprMeta[INPUT] = {
    logWarning("wrap Expr expr: " + expr)
    expressions.get(expr.getClass)
      .map(r => { 
        logWarning("mapping r is: " + r)
         val t = r.wrap(expr, conf, parent, r).asInstanceOf[BaseExprMeta[INPUT]] 
         logWarning("r wrapped is : " + t)
         t
      })
      .getOrElse {
        logWarning("rule not found")
        new RuleNotFoundExprMeta(expr, conf, parent) }
  }

abstract class WrapUnixTimeExprMeta[A <: BinaryExpression with TimeZoneAwareExpression]
   (expr: A, conf: RapidsConf,
   parent: Option[RapidsMeta[_, _, _]],
   rule: ConfKeysAndIncompat) extends UnixTimeExprMeta[A](expr, conf, parent, rule) {
    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
}

abstract class WrapGpuWindowSpecDefinitionMeta(
      windowSpec: WindowSpecDefinition,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_,_,_]],
      rule: ConfKeysAndIncompat)
  extends GpuWindowSpecDefinitionMeta(windowSpec, conf, parent, rule) {
    override val childExprs: Seq[BaseExprMeta[_]] =
       windowSpec.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
  }


  abstract class WrapUnaryExprMeta[INPUT <: UnaryExpression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      rule: ConfKeysAndIncompat)
    extends UnaryExprMeta[INPUT](expr, conf, parent, rule) with Logging {

    override val childExprs: Seq[BaseExprMeta[_]] = {
      logWarning("Tom unary expr children: " + expr.children)
      val childes =  expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
      logWarning("Tom unary expr children wrapped: " + childes)
      childes
    }
  }

abstract class WrapComplexTypeMergingExprMeta[INPUT <: ComplexTypeMergingExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends ComplexTypeMergingExprMeta[INPUT](expr, conf, parent, rule) {
    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
}


abstract class WrapString2TrimExpressionMeta[INPUT <: String2TrimExpression](
    expr: INPUT,
    trimStr: Option[Expression],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
    extends String2TrimExpressionMeta[INPUT](expr, trimStr, conf, parent, rule) {

    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
  }

abstract class WrapAggExprMeta[INPUT <: AggregateFunction](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends AggExprMeta[INPUT](expr, conf, parent, rule) {

    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
}


  abstract class WrapExprMeta[INPUT <: Expression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      rule: ConfKeysAndIncompat)
      extends ExprMeta[INPUT](expr, conf, parent, rule) {

    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
  }

abstract class WrapBinaryExprMeta[INPUT <: BinaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends BinaryExprMeta[INPUT](expr, conf, parent, rule) {

    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
}

/**
 * Base class for metadata around `TernaryExpression`.
 */
abstract class WrapTernaryExprMeta[INPUT <: TernaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends TernaryExprMeta[INPUT](expr, conf, parent, rule) {

    override val childExprs: Seq[BaseExprMeta[_]] =
       expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this))) 
}


  val expressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[Literal](
      "holds a static value from the query",
      (lit, conf, p, r) => new WrapExprMeta[Literal](lit, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuLiteral(lit.value, lit.dataType)

        // There are so many of these that we don't need to print them out.
        override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {}

        /**
         * We are overriding this method because currently we only support CalendarIntervalType
         * as a Literal
         */
        override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
            case CalendarIntervalType => true
            case x => RapidsMeta.isSupportedType(x)
          }

      }),
    expr[Signum](
      "Returns -1.0, 0.0 or 1.0 as expr is negative, 0 or positive",
      (a, conf, p, r) => new WrapUnaryExprMeta[Signum](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSignum(child)
      }),
    expr[Alias](
      "gives a column a name",
      (a, conf, p, r) => new WrapUnaryExprMeta[Alias](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuAlias(child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      }),
    expr[AttributeReference](
      "references an input column",
      (att, conf, p, r) => new BaseExprMeta[AttributeReference](att, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          att.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        // This is the only NOOP operator.  It goes away when things are bound
        override def convertToGpu(): Expression = att

        // There are so many of these that we don't need to print them out.
        override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {}
      }),
    expr[Cast](
      "convert a column of one type of data into another type",
      (cast, conf, p, r) => new CastExprMeta[Cast](cast, SparkSession.active.sessionState.conf
        .ansiEnabled, conf, p, r) {
          override val childExprs: Seq[BaseExprMeta[_]] =
                cast.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        }),
    expr[AnsiCast](
      "convert a column of one type of data into another type",
      (cast, conf, p, r) => new CastExprMeta[AnsiCast](cast, true, conf, p, r) {
          override val childExprs: Seq[BaseExprMeta[_]] =
                cast.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    expr[ToDegrees](
      "Converts radians to degrees",
      (a, conf, p, r) => new WrapUnaryExprMeta[ToDegrees](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuToDegrees = GpuToDegrees(child)
      }),
    expr[ToRadians](
      "Converts degrees to radians",
      (a, conf, p, r) => new WrapUnaryExprMeta[ToRadians](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuToRadians = GpuToRadians(child)
      }),
    expr[WindowExpression](
      "calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
              windowExpression.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    expr[SpecifiedWindowFrame](
      "specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
              windowFrame.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    expr[WindowSpecDefinition](
      "specification of a window function, indicating the partitioning-expression, the row " +
        "ordering, and the width of the window",
      (windowSpec, conf, p, r) => new WrapGpuWindowSpecDefinitionMeta(windowSpec, conf, p, r) {
        val partitionSpec: Seq[BaseExprMeta[Expression]] =
          windowSpec.partitionSpec.map(wrapExpr(_, conf, Some(this)))
        val orderSpec: Seq[BaseExprMeta[SortOrder]] =
          windowSpec.orderSpec.map(wrapExpr(_, conf, Some(this)))
        val windowFrame: BaseExprMeta[WindowFrame] =
          wrapExpr(windowSpec.frameSpecification, conf, Some(this))
      }),
    expr[CurrentRow.type](
      "Special boundary for a window frame, indicating stopping at the current row",
      (currentRow, conf, p, r) => new WrapExprMeta[CurrentRow.type](currentRow, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(currentRow)

        // CURRENT ROW needs to support NullType.
        override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
          case _: NullType => true
          case anythingElse => RapidsMeta.isSupportedType(anythingElse)
        }
      }
    ),
    expr[UnboundedPreceding.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      (unboundedPreceding, conf, p, r) =>
        new WrapExprMeta[UnboundedPreceding.type](unboundedPreceding, conf, p, r) {
          override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(unboundedPreceding)

          // UnboundedPreceding needs to support NullType.
          override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
            case _: NullType => true
            case anythingElse => RapidsMeta.isSupportedType(anythingElse)
          }
        }
    ),
    expr[UnboundedFollowing.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      (unboundedFollowing, conf, p, r) =>
        new WrapExprMeta[UnboundedFollowing.type](unboundedFollowing, conf, p, r) {
          override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(unboundedFollowing)

          // UnboundedFollowing needs to support NullType.
          override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
            case _: NullType => true
            case anythingElse => RapidsMeta.isSupportedType(anythingElse)
          }
        }
    ),
    expr[RowNumber](
      "Window function that returns the index for the row within the aggregation window",
      (rowNumber, conf, p, r) => new WrapExprMeta[RowNumber](rowNumber, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuRowNumber()
      }
    ),
    expr[UnaryMinus](
      "negate a numeric value",
      (a, conf, p, r) => new WrapUnaryExprMeta[UnaryMinus](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUnaryMinus(child)
      }),
    expr[UnaryPositive](
      "a numeric value with a + in front of it",
      (a, conf, p, r) => new WrapUnaryExprMeta[UnaryPositive](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUnaryPositive(child)
      }),
    expr[Year](
      "get the year from a date or timestamp",
      (a, conf, p, r) => new WrapUnaryExprMeta[Year](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuYear(child)
      }),
    expr[Month](
      "get the month from a date or timestamp",
      (a, conf, p, r) => new WrapUnaryExprMeta[Month](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuMonth(child)
      }),
    expr[Quarter](
      "returns the quarter of the year for date, in the range 1 to 4.",
      (a, conf, p, r) => new WrapUnaryExprMeta[Quarter](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuQuarter(child)
      }),
    expr[DayOfMonth](
      "get the day of the month from a date or timestamp",
      (a, conf, p, r) => new WrapUnaryExprMeta[DayOfMonth](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuDayOfMonth(child)
      }),
    expr[DayOfYear](
      "get the day of the year from a date or timestamp",
      (a, conf, p, r) => new WrapUnaryExprMeta[DayOfYear](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuDayOfYear(child)
      }),
    expr[Abs](
      "absolute value",
      (a, conf, p, r) => new WrapUnaryExprMeta[Abs](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child)
      }),
    expr[Acos](
      "inverse cosine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Acos](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAcos(child)
      }),
    expr[Acosh](
      "inverse hyperbolic cosine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Acosh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          if (conf.includeImprovedFloat) {
            GpuAcoshImproved(child)
          } else {
            GpuAcoshCompat(child)
          }
      }),
    expr[Asin](
      "inverse sine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Asin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAsin(child)
      }),
    expr[Asinh](
      "inverse hyperbolic sine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Asinh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          if (conf.includeImprovedFloat) {
            GpuAsinhImproved(child)
          } else {
            GpuAsinhCompat(child)
          }
      }),
    expr[Sqrt](
      "square root",
      (a, conf, p, r) => new WrapUnaryExprMeta[Sqrt](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSqrt(child)
      }),
    expr[Cbrt](
      "cube root",
      (a, conf, p, r) => new WrapUnaryExprMeta[Cbrt](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCbrt(child)
      }),
    expr[Floor](
      "floor of a number",
      (a, conf, p, r) => new WrapUnaryExprMeta[Floor](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuFloor(child)
      }),
    expr[Ceil](
      "ceiling of a number",
      (a, conf, p, r) => new WrapUnaryExprMeta[Ceil](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCeil(child)
      }),
    expr[Not](
      "boolean not operator",
      (a, conf, p, r) => new WrapUnaryExprMeta[Not](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuNot(child)
      }),
    expr[IsNull](
      "checks if a value is null",
      (a, conf, p, r) => new WrapUnaryExprMeta[IsNull](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNull(child)
      }),
    expr[IsNotNull](
      "checks if a value is not null",
      (a, conf, p, r) => new WrapUnaryExprMeta[IsNotNull](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNotNull(child)
      }),
    expr[IsNaN](
      "checks if a value is NaN",
      (a, conf, p, r) => new WrapUnaryExprMeta[IsNaN](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNan(child)
      }),
    expr[Rint](
      "Rounds up a double value to the nearest double equal to an integer",
      (a, conf, p, r) => new WrapUnaryExprMeta[Rint](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuRint(child)
      }),
    expr[BitwiseNot](
      "Returns the bitwise NOT of the operands",
      (a, conf, p, r) => new WrapUnaryExprMeta[BitwiseNot](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = {
          GpuBitwiseNot(child)
        }
      }),
    expr[AtLeastNNonNulls](
      "checks if number of non null/Nan values is greater than a given value",
      (a, conf, p, r) => new WrapExprMeta[AtLeastNNonNulls](a, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] = a.children
          .map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        def convertToGpu(): GpuExpression = {
          GpuAtLeastNNonNulls(a.n, childExprs.map(_.convertToGpu()))
        }
      }),
    expr[DateAdd](
      "Returns the date that is num_days after start_date",
      (a, conf, p, r) => new WrapBinaryExprMeta[DateAdd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateAdd(lhs, rhs)
      }
    ),
    expr[DateSub](
      "Returns the date that is num_days before start_date",
      (a, conf, p, r) => new WrapBinaryExprMeta[DateSub](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateSub(lhs, rhs)
      }
    ),
    expr[TimeSub](
      "Subtracts interval from timestamp",
      (a, conf, p, r) => new WrapBinaryExprMeta[TimeSub](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.interval match {
            case Literal(intvl: CalendarInterval, DataTypes.CalendarIntervalType) =>
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            case _ =>
              willNotWorkOnGpu("only literals are supported for intervals")
          }
          if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
            willNotWorkOnGpu("Only UTC zone id is supported")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimeSub(lhs, rhs)
      }
    ),
    expr[NaNvl](
      "evaluates to `left` iff left is not NaN, `right` otherwise.",
      (a, conf, p, r) => new WrapBinaryExprMeta[NaNvl](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuNaNvl(lhs, rhs)
      }
    ),
    expr[ShiftLeft](
      "Bitwise shift left (<<)",
      (a, conf, p, r) => new WrapBinaryExprMeta[ShiftLeft](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftLeft(lhs, rhs)
      }),
    expr[ShiftRight](
      "Bitwise shift right (>>)",
      (a, conf, p, r) => new WrapBinaryExprMeta[ShiftRight](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftRight(lhs, rhs)
      }),
    expr[ShiftRightUnsigned](
      "Bitwise unsigned shift right (>>>)",
      (a, conf, p, r) => new WrapBinaryExprMeta[ShiftRightUnsigned](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftRightUnsigned(lhs, rhs)
      }),
    expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      (a, conf, p, r) => new WrapBinaryExprMeta[BitwiseAnd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseAnd(lhs, rhs)
      }
    ),
    expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      (a, conf, p, r) => new WrapBinaryExprMeta[BitwiseOr](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseOr(lhs, rhs)
      }
    ),
    expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      (a, conf, p, r) => new WrapBinaryExprMeta[BitwiseXor](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseXor(lhs, rhs)
      }
    ),
    expr[Coalesce] (
      "Returns the first non-null argument if exists. Otherwise, null.",
      (a, conf, p, r) => new WrapExprMeta[Coalesce](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuCoalesce(childExprs.map(_.convertToGpu()))
      }
    ),
    expr[Atan](
      "inverse tangent",
      (a, conf, p, r) => new WrapUnaryExprMeta[Atan](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAtan(child)
      }),
    expr[Atanh](
      "inverse hyperbolic tangent",
      (a, conf, p, r) => new WrapUnaryExprMeta[Atanh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAtanh(child)
      }),
    expr[Cos](
      "cosine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Cos](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCos(child)
      }),
    expr[Exp](
      "Euler's number e raised to a power",
      (a, conf, p, r) => new WrapUnaryExprMeta[Exp](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuExp(child)
      }),
    expr[Expm1](
      "Euler's number e raised to a power minus 1",
      (a, conf, p, r) => new WrapUnaryExprMeta[Expm1](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuExpm1(child)
      }),
    expr[InitCap]("Returns str with the first letter of each word in uppercase. " +
      "All other letters are in lowercase",
      (a, conf, p, r) => new WrapUnaryExprMeta[InitCap](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuInitCap(child)
      }).incompat(CASE_MODIFICATION_INCOMPAT + " Spark also only sees the space character as " +
      "a word deliminator, but this uses more white space characters."),
    expr[Log](
      "natural log",
      (a, conf, p, r) => new WrapUnaryExprMeta[Log](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLog(child)
      }),
    expr[Log1p](
      "natural log 1 + expr",
      (a, conf, p, r) => new WrapUnaryExprMeta[Log1p](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLog(GpuAdd(child, GpuLiteral(1d, DataTypes.DoubleType)))
      }),
    expr[Log2](
      "log base 2",
      (a, conf, p, r) => new WrapUnaryExprMeta[Log2](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(2d, DataTypes.DoubleType))
      }),
    expr[Log10](
      "log base 10",
      (a, conf, p, r) => new WrapUnaryExprMeta[Log10](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(10d, DataTypes.DoubleType))
      }),
    expr[Logarithm](
      "log variable base",
      (a, conf, p, r) => new WrapBinaryExprMeta[Logarithm](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          // the order of the parameters is transposed intentionally
          GpuLogarithm(rhs, lhs)
        }
      }),
    expr[Sin](
      "sine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Sin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSin(child)
      }),
    expr[Sinh](
      "hyperbolic sine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Sinh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSinh(child)
      }),
    expr[Cosh](
      "hyperbolic cosine",
      (a, conf, p, r) => new WrapUnaryExprMeta[Cosh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCosh(child)
      }),
    expr[Cot](
      "Returns the cotangent",
      (a, conf, p, r) => new WrapUnaryExprMeta[Cot](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCot(child)
      }),
    expr[Tanh](
      "hyperbolic tangent",
      (a, conf, p, r) => new WrapUnaryExprMeta[Tanh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuTanh(child)
      }),
    expr[Tan](
      "tangent",
      (a, conf, p, r) => new WrapUnaryExprMeta[Tan](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuTan(child)
      }),
    expr[NormalizeNaNAndZero](
      "normalize nan and zero",
      (a, conf, p, r) => new WrapUnaryExprMeta[NormalizeNaNAndZero](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuNormalizeNaNAndZero(child)
      }),
    expr[KnownFloatingPointNormalized](
      "tag to prevent redundant normalization",
      (a, conf, p, r) => new WrapUnaryExprMeta[KnownFloatingPointNormalized](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuKnownFloatingPointNormalized(child)
      }),
    expr[DateDiff]("datediff", (a, conf, p, r) =>
      new WrapBinaryExprMeta[DateDiff](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuDateDiff(lhs, rhs)
        }
    }),
    expr[ToUnixTimestamp](
      "Returns the UNIX timestamp of the given time",
      (a, conf, p, r) => new WrapUnixTimeExprMeta[ToUnixTimestamp](a, conf, p, r){
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          if (conf.isImprovedTimestampOpsEnabled) {
            // passing the already converted strf string for a little optimization
            GpuToUnixTimestampImproved(lhs, rhs, strfFormat)
          } else {
            GpuToUnixTimestamp(lhs, rhs, strfFormat)
          }
        }
      })
      .incompat("Incorrectly formatted strings and bogus dates produce garbage data" +
        " instead of null"),
    expr[UnixTimestamp](
      "Returns the UNIX timestamp of current or specified time",
      (a, conf, p, r) => new WrapUnixTimeExprMeta[UnixTimestamp](a, conf, p, r){
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          if (conf.isImprovedTimestampOpsEnabled) {
            // passing the already converted strf string for a little optimization
            GpuUnixTimestampImproved(lhs, rhs, strfFormat)
          } else {
            GpuUnixTimestamp(lhs, rhs, strfFormat)
          }
        }
      })
      .incompat("Incorrectly formatted strings and bogus dates produce garbage data" +
        " instead of null"),
    expr[Hour](
      "Returns the hour component of the string/timestamp.",
      (a, conf, p, r) => new WrapUnaryExprMeta[Hour](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
            willNotWorkOnGpu("Only UTC zone id is supported")
          }
        }

        override def convertToGpu(expr: Expression): GpuExpression = GpuHour(expr)
      }),
    expr[Minute](
      "Returns the minute component of the string/timestamp.",
      (a, conf, p, r) => new WrapUnaryExprMeta[Minute](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
            willNotWorkOnGpu("Only UTC zone id is supported")
          }
        }

        override def convertToGpu(expr: Expression): GpuExpression =
          GpuMinute(expr)
      }),
    expr[Second](
      "Returns the second component of the string/timestamp.",
      (a, conf, p, r) => new WrapUnaryExprMeta[Second](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
            if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
              willNotWorkOnGpu("Only UTC zone id is supported")
            }
        }

        override def convertToGpu(expr: Expression): GpuExpression =
          GpuSecond(expr)
      }),
    expr[WeekDay](
      "Returns the day of the week (0 = Monday...6=Sunday)",
      (a, conf, p, r) => new WrapUnaryExprMeta[WeekDay](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuWeekDay(expr)
      }),
    expr[DayOfWeek](
      "Returns the day of the week (1 = Sunday...7=Saturday)",
      (a, conf, p, r) => new WrapUnaryExprMeta[DayOfWeek](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuDayOfWeek(expr)
      }),
    expr[LastDay](
      "Returns the last day of the month which the date belongs to",
      (a, conf, p, r) => new WrapUnaryExprMeta[LastDay](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuLastDay(expr)
      }),
    expr[FromUnixTime](
      "get the String from a unix timestamp",
      (a, conf, p, r) => new WrapUnixTimeExprMeta[FromUnixTime](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          // passing the already converted strf string for a little optimization
          GpuFromUnixTime(lhs, rhs, strfFormat)
        }
      }),
    expr[Pmod](
      "pmod",
      (a, conf, p, r) => new WrapBinaryExprMeta[Pmod](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuPmod(lhs, rhs)
      }),
    expr[Add](
      "addition",
      (a, conf, p, r) => new WrapBinaryExprMeta[Add](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuAdd(lhs, rhs)
      }),
    expr[Subtract](
      "subtraction",
      (a, conf, p, r) => new WrapBinaryExprMeta[Subtract](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuSubtract(lhs, rhs)
      }),
    expr[Multiply](
      "multiplication",
      (a, conf, p, r) => new WrapBinaryExprMeta[Multiply](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuMultiply(lhs, rhs)
      }),
    expr[And](
      "logical and",
      (a, conf, p, r) => new WrapBinaryExprMeta[And](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuAnd(lhs, rhs)
      }),
    expr[Or](
      "logical or",
      (a, conf, p, r) => new WrapBinaryExprMeta[Or](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuOr(lhs, rhs)
      }),
    expr[EqualNullSafe](
      "check if the values are equal including nulls <=>",
      (a, conf, p, r) => new WrapBinaryExprMeta[EqualNullSafe](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEqualNullSafe(lhs, rhs)
      }),
    expr[EqualTo](
      "check if the values are equal",
      (a, conf, p, r) => new WrapBinaryExprMeta[EqualTo](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEqualTo(lhs, rhs)
      }),
    expr[GreaterThan](
      "> operator",
      (a, conf, p, r) => new WrapBinaryExprMeta[GreaterThan](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuGreaterThan(lhs, rhs)
      }),
    expr[GreaterThanOrEqual](
      ">= operator",
      (a, conf, p, r) => new WrapBinaryExprMeta[GreaterThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuGreaterThanOrEqual(lhs, rhs)
      }),
    expr[In](
      "IN operator",
      (in, conf, p, r) => new WrapExprMeta[In](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val unaliased = in.list.map(extractLit)
          if (!unaliased.forall(_.isDefined)) {
            willNotWorkOnGpu("only literals are supported")
          }
          val hasNullLiteral = unaliased.exists {
            case Some(l) => l.value == null
            case _ => false
          }
          if (hasNullLiteral) {
            willNotWorkOnGpu("nulls are not supported")
          }
        }
        override def convertToGpu(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.list.asInstanceOf[Seq[Literal]])
      }),
    expr[InSet](
      "INSET operator",
      (in, conf, p, r) => new WrapExprMeta[InSet](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (in.hset.contains(null)) {
            willNotWorkOnGpu("nulls are not supported")
          }
          val literalTypes = in.hset.map(Literal(_).dataType).toSeq
          if (!areAllSupportedTypes(literalTypes:_*)) {
            val unsupported = literalTypes.filter(!areAllSupportedTypes(_)).mkString(", ")
            willNotWorkOnGpu(s"unsupported literal types: $unsupported")
          }
        }
        override def convertToGpu(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.hset.map(Literal(_)).toSeq)
      }),
    expr[LessThan](
      "< operator",
      (a, conf, p, r) => new WrapBinaryExprMeta[LessThan](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLessThan(lhs, rhs)
      }),
    expr[LessThanOrEqual](
      "<= operator",
      (a, conf, p, r) => new WrapBinaryExprMeta[LessThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLessThanOrEqual(lhs, rhs)
      }),
    expr[CaseWhen](
      "CASE WHEN expression",
      (a, conf, p, r) => new WrapExprMeta[CaseWhen](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val anyLit = a.branches.exists { case (predicate, _) => isLit(predicate) }
          if (anyLit) {
            willNotWorkOnGpu("literal predicates are not supported")
          }
        }
        override def convertToGpu(): GpuExpression = {
          val branches = childExprs.grouped(2).flatMap {
            case Seq(cond, value) => Some((cond.convertToGpu(), value.convertToGpu()))
            case Seq(_) => None
          }.toArray.toSeq  // force materialization to make the seq serializable
          val elseValue = if (childExprs.size % 2 != 0) {
            Some(childExprs.last.convertToGpu())
          } else {
            None
          }
          GpuCaseWhen(branches, elseValue)
        }
      }),
    expr[If](
      "IF expression",
      (a, conf, p, r) => new WrapExprMeta[If](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (isLit(a.predicate)) {
            willNotWorkOnGpu(s"literal predicate ${a.predicate} is not supported")
          }
        }
        override def convertToGpu(): GpuExpression = {
          val boolExpr :: trueExpr :: falseExpr :: Nil = childExprs.map(_.convertToGpu())
          GpuIf(boolExpr, trueExpr, falseExpr)
        }
      }),
    expr[Pow](
      "lhs ^ rhs",
      (a, conf, p, r) => new WrapBinaryExprMeta[Pow](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuPow(lhs, rhs)
      }),
    expr[Divide](
      "division",
      (a, conf, p, r) => new WrapBinaryExprMeta[Divide](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDivide(lhs, rhs)
      }),
    expr[IntegralDivide](
      "division with a integer result",
      (a, conf, p, r) => new WrapBinaryExprMeta[IntegralDivide](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuIntegralDivide(lhs, rhs)
      }),
    expr[Remainder](
      "remainder or modulo",
      (a, conf, p, r) => new WrapBinaryExprMeta[Remainder](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuRemainder(lhs, rhs)
      }),
    expr[AggregateExpression](
      "aggregate expression",
      (a, conf, p, r) => new WrapExprMeta[AggregateExpression](a, conf, p, r) {
        private val filter: Option[BaseExprMeta[_]] =
          a.filter.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val childrenExprMeta: Seq[BaseExprMeta[Expression]] =
          a.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] = if (filter.isDefined) {
          childrenExprMeta :+ filter.get
        } else {
          childrenExprMeta
        }
       override def convertToGpu(): GpuExpression = {
         // handle the case AggregateExpression has the resultIds parameter where its
         // Seq[ExprIds] instead of single ExprId.
         val resultId = try {
            val resultMethod = a.getClass.getMethod("resultId")
            resultMethod.invoke(a).asInstanceOf[ExprId]
          } catch {
            case e: Exception =>
              val resultMethod = a.getClass.getMethod("resultIds")
              resultMethod.invoke(a).asInstanceOf[Seq[ExprId]](0)
          }
          GpuAggregateExpression(childExprs(0).convertToGpu().asInstanceOf[GpuAggregateFunction],
            a.mode, a.isDistinct, filter.map(_.convertToGpu()), resultId)
       }
      }),
    expr[SortOrder](
      "sort order",
      (a, conf, p, r) => new BaseExprMeta[SortOrder](a, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          a.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        // One of the few expressions that are not replaced with a GPU version
        override def convertToGpu(): Expression =
          a.withNewChildren(childExprs.map(_.convertToGpu()))
      }),
    expr[Count](
      "count aggregate operator",
      (count, conf, p, r) => new WrapExprMeta[Count](count, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (count.children.size > 1) {
            willNotWorkOnGpu("count of multiple columns not supported")
          }
        }

        override def convertToGpu(): GpuExpression = GpuCount(childExprs.map(_.convertToGpu()))
      }),
    expr[Max](
      "max aggregate operator",
      (max, conf, p, r) => new WrapAggExprMeta[Max](max, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = max.child.dataType
          if (conf.hasNans && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("Max aggregation on floating point columns that can contain NaNs " +
              "will compute incorrect results. If it is known that there are no NaNs, set " +
              s" ${RapidsConf.HAS_NANS} to false.")
          }
        }
        override def convertToGpu(child: Expression): GpuExpression = GpuMax(child)
      }),
    expr[Min](
      "min aggregate operator",
      (a, conf, p, r) => new WrapAggExprMeta[Min](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = a.child.dataType
          if (conf.hasNans && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("Min aggregation on floating point columns that can contain NaNs " +
              "will compute incorrect results. If it is known that there are no NaNs, set " +
              s" ${RapidsConf.HAS_NANS} to false.")
          }
        }
        override def convertToGpu(child: Expression): GpuExpression = GpuMin(child)
      }),
    expr[First](
      "first aggregate operator",
      (a, conf, p, r) => new WrapExprMeta[First](a, conf, p, r) {
        val child: BaseExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
        val ignoreNulls: BaseExprMeta[_] =
          GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
        override val childExprs: Seq[BaseExprMeta[_]] = Seq(child, ignoreNulls)

        override def convertToGpu(): GpuExpression =
          GpuFirst(child.convertToGpu(), ignoreNulls.convertToGpu())
      }),
    expr[Last](
      "last aggregate operator",
      (a, conf, p, r) => new WrapExprMeta[Last](a, conf, p, r) {
        val child: BaseExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
        val ignoreNulls: BaseExprMeta[_] =
          GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
        override val childExprs: Seq[BaseExprMeta[_]] = Seq(child, ignoreNulls)

        override def convertToGpu(): GpuExpression =
          GpuLast(child.convertToGpu(), ignoreNulls.convertToGpu())
      }),
    expr[Sum](
      "sum aggregate operator",
      (a, conf, p, r) => new WrapAggExprMeta[Sum](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = a.child.dataType
          if (!conf.isFloatAggEnabled && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("the GPU will sum floating point values in" +
              " parallel and the result is not always identical each time. This can cause some" +
              " Spark queries to produce an incorrect answer if the value is computed more than" +
              " once as part of the same query.  To enable this anyways set" +
              s" ${RapidsConf.ENABLE_FLOAT_AGG} to true.")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = GpuSum(child)
      }),
    expr[Average](
      "average aggregate operator",
      (a, conf, p, r) => new WrapAggExprMeta[Average](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = a.child.dataType
          if (!conf.isFloatAggEnabled && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("the GPU will sum floating point values in" +
              " parallel to compute an average and the result is not always identical each time." +
              " This can cause some Spark queries to produce an incorrect answer if the value is" +
              " computed more than once as part of the same query. To enable this anyways set" +
              s" ${RapidsConf.ENABLE_FLOAT_AGG} to true")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = GpuAverage(child)
      }),
    expr[Rand](
      "Generate a random column with i.i.d. uniformly distributed values in [0, 1)",
      (a, conf, p, r) => new WrapUnaryExprMeta[Rand](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuRand(child)
      }),
    expr[SparkPartitionID] (
      "Returns the current partition id.",
      (a, conf, p, r) => new WrapExprMeta[SparkPartitionID](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuSparkPartitionID()
      }
    ),
    expr[MonotonicallyIncreasingID] (
      "Returns monotonically increasing 64-bit integers.",
      (a, conf, p, r) => new WrapExprMeta[MonotonicallyIncreasingID](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuMonotonicallyIncreasingID()
      }
    ),
    expr[InputFileName] (
      "Returns the name of the file being read, or empty string if not available.",
      (a, conf, p, r) => new WrapExprMeta[InputFileName](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileName()
      }
    ),
    expr[InputFileBlockStart] (
      "Returns the start offset of the block being read, or -1 if not available.",
      (a, conf, p, r) => new WrapExprMeta[InputFileBlockStart](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileBlockStart()
      }
    ),
    expr[InputFileBlockLength] (
      "Returns the length of the block being read, or -1 if not available.",
      (a, conf, p, r) => new WrapExprMeta[InputFileBlockLength](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileBlockLength()
      }
    ),
    expr[Upper](
      "String uppercase operator",
      (a, conf, p, r) => new WrapUnaryExprMeta[Upper](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUpper(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Lower](
      "String lowercase operator",
      (a, conf, p, r) => new WrapUnaryExprMeta[Lower](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLower(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[StringLocate](
      "Substring search operator",
      (in, conf, p, r) => new WrapTernaryExprMeta[StringLocate](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!in.children(0).isInstanceOf[Literal] || !in.children(2).isInstanceOf[Literal]) {
            willNotWorkOnGpu("only literal search parameters supported")
          } else if (in.children(1).isInstanceOf[Literal]) {
            willNotWorkOnGpu("only operating on columns supported")
          }
        }
        override def convertToGpu(
            val0: Expression,
            val1: Expression,
            val2: Expression): GpuExpression =
          GpuStringLocate(val0, val1, val2)
      }),
    expr[Substring](
      "Substring operator",
      (in, conf, p, r) => new WrapTernaryExprMeta[Substring](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isLit(in.children(1)) || !isLit(in.children(2))) {
            willNotWorkOnGpu("only literal parameters supported for Substring position and " +
              "length parameters")
          }
        }

        override def convertToGpu(
            column: Expression,
            position: Expression,
            length: Expression): GpuExpression =
          GpuSubstring(column, position, length)
      }),
    expr[SubstringIndex](
      "substring_index operator",
      (in, conf, p, r) => new SubstringIndexMeta(in, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
              in.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    expr[StringReplace](
      "StringReplace operator",
      (in, conf, p, r) => new WrapTernaryExprMeta[StringReplace](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(in.children(1)) || !isStringLit(in.children(2))) {
            willNotWorkOnGpu("only literal parameters supported for string literal target and " +
              "replace parameters")
          }
        }

        override def convertToGpu(
            column: Expression,
            target: Expression,
            replace: Expression): GpuExpression =
          GpuStringReplace(column, target, replace)
      }),
    expr[StringTrim](
      "StringTrim operator",
      (in, conf, p, r) => new WrapString2TrimExpressionMeta[StringTrim](in, in.trimStr, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            target: Option[Expression] = None): GpuExpression =
          GpuStringTrim(column, target)
      }),
    expr[StringTrimLeft](
      "StringTrimLeft operator",
      (in, conf, p, r) =>
        new WrapString2TrimExpressionMeta[StringTrimLeft](in, in.trimStr, conf, p, r) {
          override def convertToGpu(
            column: Expression,
            target: Option[Expression] = None): GpuExpression =
            GpuStringTrimLeft(column, target)
        }),
    expr[StringTrimRight](
      "StringTrimRight operator",
      (in, conf, p, r) =>
        new WrapString2TrimExpressionMeta[StringTrimRight](in, in.trimStr, conf, p, r) {
          override def convertToGpu(
              column: Expression,
              target: Option[Expression] = None): GpuExpression =
            GpuStringTrimRight(column, target)
        }
      ),
    expr[StartsWith](
      "Starts With",
      (a, conf, p, r) => new WrapBinaryExprMeta[StartsWith](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for startsWith")
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStartsWith(lhs, rhs)
      }),
    expr[EndsWith](
      "Ends With",
      (a, conf, p, r) => new WrapBinaryExprMeta[EndsWith](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for endsWith")
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEndsWith(lhs, rhs)
      }),
    expr[Concat](
      "String Concatenate NO separator",
      (a, conf, p, r) => new WrapComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {}
        override def convertToGpu(child: Seq[Expression]): GpuExpression = GpuConcat(child)
      }),
    expr[Contains](
      "Contains",
      (a, conf, p, r) => new WrapBinaryExprMeta[Contains](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for Contains right hand side search" +
              " parameter")
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuContains(lhs, rhs)
      }),
    expr[Like](
      "Like",
      (a, conf, p, r) => new WrapBinaryExprMeta[Like](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for Like right hand side search" +
              " parameter")
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLike(lhs, rhs, a.escapeChar)
      }),
    expr[RegExpReplace](
      "RegExpReplace",
      (a, conf, p, r) => new WrapTernaryExprMeta[RegExpReplace](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isLit(a.rep)) {
            willNotWorkOnGpu("Only literal values are supported for replacement string")
          }
          if (isNullOrEmptyOrRegex(a.regexp)) {
            willNotWorkOnGpu(
              "Only non-null, non-empty String literals that are not regex patterns " +
                "are supported by RegExpReplace on the GPU")
          }
        }
        override def convertToGpu(lhs: Expression, regexp: Expression,
          rep: Expression): GpuExpression = GpuStringReplace(lhs, regexp, rep)
      }),
    expr[Length](
      "String Character Length",
      (a, conf, p, r) => new WrapUnaryExprMeta[Length](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLength(child)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  def wrapScan[INPUT <: Scan](
      scan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): ScanMeta[INPUT] =
    scans.get(scan.getClass)
      .map(r => r.wrap(scan, conf, parent, r).asInstanceOf[ScanMeta[INPUT]])
      .getOrElse(new RuleNotFoundScanMeta(scan, conf, parent))

  val scans : Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new ScanMeta[CSVScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuCSVScan.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuCSVScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf.maxReadBatchSizeRows,
            conf.maxReadBatchSizeBytes)
      }),
    scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new ScanMeta[ParquetScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuParquetScan.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuParquetScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.pushedFilters,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf)
      }),
    scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new ScanMeta[OrcScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit =
          GpuOrcScan.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuOrcScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.pushedFilters,
            a.partitionFilters,
            a.dataFilters,
            conf)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  def wrapPart[INPUT <: Partitioning](
      part: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): PartMeta[INPUT] =
    parts.get(part.getClass)
      .map(r => r.wrap(part, conf, parent, r).asInstanceOf[PartMeta[INPUT]])
      .getOrElse(new RuleNotFoundPartMeta(part, conf, parent))

  val parts : Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] = Seq(
    part[HashPartitioning](
      "Hash based partitioning",
      (hp, conf, p, r) => new PartMeta[HashPartitioning](hp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          hp.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override def convertToGpu(): GpuPartitioning =
          GpuHashPartitioning(childExprs.map(_.convertToGpu()), hp.numPartitions)
      }),
    part[RangePartitioning]( "Range Partitioning",
      (rp, conf, p, r) => new PartMeta[RangePartitioning](rp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          rp.ordering.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override def convertToGpu(): GpuPartitioning = {
          if (rp.numPartitions > 1) {
            val gpuOrdering = childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]]
            val tmp = gpuOrdering.flatMap { ord =>
              ord.child.references.map { field =>
                StructField(field.name, field.dataType)
              }
            }
            val schema = new StructType(tmp.toArray)

            GpuRangePartitioning(gpuOrdering, rp.numPartitions, new GpuRangePartitioner, schema)
          } else {
            GpuSinglePartitioning(childExprs.map(_.convertToGpu()))
          }
        }
      }),
    part[RoundRobinPartitioning]( "Round Robin Partitioning",
      (rrp, conf, p, r) => new PartMeta[RoundRobinPartitioning](rrp, conf, p, r) {
        override def convertToGpu(): GpuPartitioning = {
          GpuRoundRobinPartitioning(rrp.numPartitions)
        }
      }),
    part[SinglePartition.type]( "Single Partitioning",
      (sp, conf, p, r) => new PartMeta[SinglePartition.type](sp, conf, p, r) {
        override val childExprs: Seq[ExprMeta[_]] = Seq.empty[ExprMeta[_]]
        override def convertToGpu(): GpuPartitioning = {
          GpuSinglePartitioning(childExprs.map(_.convertToGpu()))
        }
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Partitioning]), r)).toMap

  def wrapDataWriteCmds[INPUT <: DataWritingCommand](
      writeCmd: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): DataWritingCommandMeta[INPUT] =
    dataWriteCmds.get(writeCmd.getClass)
      .map(r => r.wrap(writeCmd, conf, parent, r).asInstanceOf[DataWritingCommandMeta[INPUT]])
      .getOrElse(new RuleNotFoundDataWritingCommandMeta(writeCmd, conf, parent))

  val dataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = Seq(
    dataWriteCmd[InsertIntoHadoopFsRelationCommand](
      "Write to Hadoop FileSystem",
      (a, conf, p, r) => new InsertIntoHadoopFsRelationCommandMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  def wrapPlan[INPUT <: SparkPlan](
      plan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): SparkPlanMeta[INPUT]  =
    execs.get(plan.getClass)
      .map(r => r.wrap(plan, conf, parent, r).asInstanceOf[SparkPlanMeta[INPUT]])
      .getOrElse(new RuleNotFoundSparkPlanMeta(plan, conf, parent))

  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    exec[GenerateExec] (
      "The backend for operations that generate more output rows than input rows like explode.",
      (gen, conf, p, r) => new SparkPlanMeta(gen, conf, p, r) {

      override val childPlans: Seq[SparkPlanMeta[_]] =
          gen.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

  private def exprsFromArray(data: ArrayData, dataType: DataType): Seq[BaseExprMeta[Expression]] = {
    (0 until data.numElements()).map { i =>
      Literal(data.get(i, dataType), dataType).asInstanceOf[Expression]
    }.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  }
          private val arrayExprs =  gen.generator match {
            case PosExplode(CreateArray(exprs, _)) =>
              // This bypasses the check to see if we can support an array or not.
              // and the posexplode/explode which is going to be built into this one...
              exprs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            case PosExplode(Literal(data, ArrayType(baseType, _))) =>
              exprsFromArray(data.asInstanceOf[ArrayData], baseType)
            case PosExplode(Alias(Literal(data, ArrayType(baseType, _)), _)) =>
              exprsFromArray(data.asInstanceOf[ArrayData], baseType)
            case Explode(CreateArray(exprs, _)) =>
              exprs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            case Explode(Literal(data, ArrayType(baseType, _))) =>
              exprsFromArray(data.asInstanceOf[ArrayData], baseType)
            case Explode(Alias(Literal(data, ArrayType(baseType, _)), _)) =>
              exprsFromArray(data.asInstanceOf[ArrayData], baseType)
            case _ => Seq.empty
          }

      override val childExprs: Seq[BaseExprMeta[_]] = arrayExprs

  override def tagPlanForGpu(): Unit = {
    // We can only run on the GPU if we are doing a posexplode of an array we are generating
    // right now
    gen.generator match {
      case PosExplode(CreateArray(_, _)) => // Nothing
      case PosExplode(Literal(_, ArrayType(_, _))) => // Nothing
      case PosExplode(Alias(Literal(_, ArrayType(_, _)), _)) => // Nothing
      case Explode(CreateArray(_, _)) => // Nothing
      case Explode(Literal(_, ArrayType(_, _))) => // Nothing
      case Explode(Alias(Literal(_, ArrayType(_, _)), _)) => // Nothing
      case _ => willNotWorkOnGpu("Only posexplode of a created array is currently supported")
    }

    if (gen.outer) {
      willNotWorkOnGpu("outer is not currently supported")
    }
  }
        /**
         * Convert what this wraps to a GPU enabled version.
         */
        override def convertToGpu(): GpuExec = {
          GpuGenerateExec(
            gen.generator.isInstanceOf[PosExplode],
            arrayExprs.map(_.convertToGpu()),
            gen.requiredChildOutput,
            gen.generatorOutput,
            childPlans.head.convertIfNeeded())
        }
        }),
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      (proj, conf, p, r) => {
        new SparkPlanMeta[ProjectExec](proj, conf, p, r) {
          override val childPlans: Seq[SparkPlanMeta[_]] =
            proj.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] =
            proj.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          private def exprsFromArray(data: ArrayData, dataType: DataType): Seq[BaseExprMeta[Expression]] = {
            (0 until data.numElements()).map { i =>
              Literal(data.get(i, dataType), dataType).asInstanceOf[Expression]
            }.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          }
    logWarning("Tom new in project exec childplans: " + childPlans + " children: " + proj.children + " exprs: " + childExprs)

          override def convertToGpu(): GpuExec =
            GpuProjectExec(childExprs.map(_.convertToGpu()), childPlans(0).convertIfNeeded())
        }
      }),
    exec[BatchScanExec](
      "The backend for most file input",
      (p, conf, parent, r) => new SparkPlanMeta[BatchScanExec](p, conf, parent, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          p.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          p.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childScans: scala.Seq[ScanMeta[_]] =
          Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuBatchScanExec(p.output, childScans(0).convertToGpu())
      }),
    exec[CoalesceExec](
      "The backend for the dataframe coalesce method",
      (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          coalesce.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          coalesce.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override def convertToGpu(): GpuExec =
          GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
      }),
    exec[DataWritingCommandExec](
      "Writing data",
      (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          p.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          p.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
          Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
            childPlans.head.convertIfNeeded())
      }),
    exec[FileSourceScanExec](
      "Reading data from files, often from Hive tables",
      (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          fsse.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        // partition filters and data filters are not run on the GPU
        override val childExprs: Seq[ExprMeta[_]] = Seq.empty

        override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

        override def convertToGpu(): GpuExec = {
          val newRelation = HadoopFsRelation(
            wrapped.relation.location,
            wrapped.relation.partitionSchema,
            wrapped.relation.dataSchema,
            wrapped.relation.bucketSpec,
            GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
            wrapped.relation.options)(wrapped.relation.sparkSession)
          GpuFileSourceScanExec(
            newRelation,
            wrapped.output,
            wrapped.requiredSchema,
            wrapped.partitionFilters,
            wrapped.optionalBucketSet,
            wrapped.dataFilters,
            wrapped.tableIdentifier)
        }
      }),
    exec[LocalLimitExec](
      "Per-partition limiting of results",
      (localLimitExec, conf, p, r) =>
        new SparkPlanMeta[LocalLimitExec](localLimitExec, conf, p, r) {
          override val childPlans: Seq[SparkPlanMeta[_]] =
            localLimitExec.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] =
            localLimitExec.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override def convertToGpu(): GpuExec =
            GpuLocalLimitExec(localLimitExec.limit, childPlans(0).convertIfNeeded())
        }),
    exec[GlobalLimitExec](
      "Limiting of results across partitions",
      (globalLimitExec, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimitExec, conf, p, r) {
          override val childPlans: Seq[SparkPlanMeta[_]] =
            globalLimitExec.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] =
            globalLimitExec.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(globalLimitExec.limit, childPlans(0).convertIfNeeded())
        }),
    exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      (collectLimitExec, conf, p, r) => new GpuCollectLimitMeta(collectLimitExec, conf, p, r) {

        override val childPlans: Seq[SparkPlanMeta[_]] =
          collectLimitExec.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          collectLimitExec.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childParts: scala.Seq[PartMeta[_]] =
          Seq(GpuOverrides.wrapPart(collectLimitExec.outputPartitioning, conf, Some(this)))
      }),
    exec[FilterExec](
      "The backend for most filter statements",
      (filter, conf, p, r) => new SparkPlanMeta[FilterExec](filter, conf, p, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          filter.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          filter.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override def convertToGpu(): GpuExec =
          GpuFilterExec(childExprs(0).convertToGpu(), childPlans(0).convertIfNeeded())
      }),
    exec[ShuffleExchangeExec](
      "The backend for most data being exchanged between processes",
      (shuffle, conf, p, r) => new GpuShuffleMeta(shuffle, conf, p, r) {
        // Some kinds of Partitioning are a type of expression, but Partitioning itself is not
        // so don't let them leak through as expressions
        override val childExprs: scala.Seq[ExprMeta[_]] = Seq.empty
        override val childParts: scala.Seq[PartMeta[_]] =
          Seq(GpuOverrides.wrapPart(shuffle.outputPartitioning, conf, Some(this)))

        override val childPlans: Seq[SparkPlanMeta[_]] =
          shuffle.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuShuffleExchangeExec(childParts(0).convertToGpu(),
            childPlans(0).convertIfNeeded(),
            shuffle.canChangeNumPartitions)
      }),
    exec[UnionExec](
      "The backend for the union operator",
      (union, conf, p, r) => new SparkPlanMeta[UnionExec](union, conf, p, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          union.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          union.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override def convertToGpu(): GpuExec =
          GpuUnionExec(childPlans.map(_.convertIfNeeded()))
      }),
    exec[BroadcastExchangeExec](
      "The backend for broadcast exchange of data",
      (exchange, conf, p, r) => new GpuBroadcastMeta(exchange, conf, p, r) {
override val childPlans: Seq[SparkPlanMeta[_]] =
    exchange.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] =
    exchange.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    exec[BroadcastHashJoinExec](
      "Implementation of join using broadcast data",
      (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r) {
        val leftKeys: Seq[BaseExprMeta[_]] =
          join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val rightKeys: Seq[BaseExprMeta[_]] =
          join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childPlans: Seq[SparkPlanMeta[_]] =
    join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
       override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition
      }),
    exec[ShuffledHashJoinExec](
      "Implementation of join using hashed shuffled data",
      (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r) {
  override val childPlans: Seq[SparkPlanMeta[_]] =
    join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))


        val leftKeys: Seq[BaseExprMeta[_]] =
          join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val rightKeys: Seq[BaseExprMeta[_]] =
          join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition
      }),
    exec[BroadcastNestedLoopJoinExec](
      "Implementation of join using brute force",
      (join, conf, p, r) => new GpuBroadcastNestedLoopJoinMeta(join, conf, p, r) {
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childPlans: Seq[SparkPlanMeta[_]] =
    join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq
  override def convertToGpu(): GpuExec = {
    val left = childPlans.head.convertIfNeeded()
    val right = childPlans(1).convertIfNeeded()
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSide = join.buildSide match {
      case BuildLeft => left
      case BuildRight => right
    }
    if (!buildSide.isInstanceOf[GpuBroadcastExchangeExec]) {
      throw new IllegalStateException("the broadcast must be on the GPU too")
    }
    GpuBroadcastNestedLoopJoinExec(
      left, right, join.buildSide,
      join.joinType,
      condition.map(_.convertToGpu()),
      conf.gpuTargetBatchSizeBytes)
  }
      })
        .disabledByDefault("large joins can cause out of memory errors"),
    exec[SortMergeJoinExec](
      "Sort merge join, replacing with shuffled hash join",
      (join, conf, p, r) => new GpuHashJoinBaseMeta(join, conf, p, r) {

        val leftKeys: Seq[BaseExprMeta[_]] =
          join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val rightKeys: Seq[BaseExprMeta[_]] =
          join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val condition: Option[BaseExprMeta[_]] = join.condition.map(
          GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition
        override val childPlans: Seq[SparkPlanMeta[_]] =
          join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    
    logWarning("Tom new in sort merge joinexec left keys: " + leftKeys + " childplans: " + childPlans + " join children: " + join.children)
    // Use conditions from Hash Join
    GpuHashJoin.tagJoin(this, join.joinType, join.leftKeys, join.rightKeys, join.condition)

    if (!conf.enableReplaceSortMergeJoin) {
      willNotWorkOnGpu(s"Not replacing sort merge join with hash join, " +
        s"see ${RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key}")
    }

    // make sure this is last check - if this is SortMergeJoin, the children can be Sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // ShuffleHashJoin
    if (canThisBeReplaced) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu(s"can't replace sortMergeJoin because one of the SortExec's before " +
              s"can't be replaced.")
          } else {
            plan.shouldBeRemoved("removing SortExec as part replacing sortMergeJoin with " +
              s"shuffleHashJoin")
          }
        }
      }
    }
  }

  override def convertToGpu(): GpuExec = {
    logWarning("Tom 5 sort merge join exec: ")
    ShimLoader.getGpuShuffledHashJoinShims(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      BuildRight, // just hardcode one side
      condition.map(_.convertToGpu()),
      childPlans(0).convertIfNeeded(),
      childPlans(1).convertIfNeeded())
  }
      }),
    exec[HashAggregateExec](
      "The backend for hash based aggregations",
      (agg, conf, p, r) => new GpuHashAggregateMeta(agg, conf, p, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          agg.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

        private val requiredChildDistributionExpressions: Option[Seq[BaseExprMeta[_]]] =
          agg.requiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
        private val groupingExpressions: Seq[BaseExprMeta[_]] =
          agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val aggregateExpressions: Seq[BaseExprMeta[_]] =
          agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val aggregateAttributes: Seq[BaseExprMeta[_]] =
          agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val resultExpressions: Seq[BaseExprMeta[_]] =
          agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] =
          requiredChildDistributionExpressions.getOrElse(Seq.empty) ++
            groupingExpressions ++
            aggregateExpressions ++
            aggregateAttributes ++
            resultExpressions

  override def tagPlanForGpu(): Unit = {
    if (agg.groupingExpressions.isEmpty) {
      // first/last reductions not supported yet
      if (agg.aggregateExpressions.exists(e => e.aggregateFunction.isInstanceOf[First] ||
        e.aggregateFunction.isInstanceOf[Last])) {
        willNotWorkOnGpu("First/Last reductions are not supported on GPU")
      }
    }
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }
    val hashAggMode = agg.aggregateExpressions.map(_.mode).distinct
    val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
    if (!hashAggReplaceMode.equals("all")) {
      hashAggReplaceMode match {
        case "partial" => if (hashAggMode.contains(Final) || hashAggMode.contains(Complete)) {
          // replacing only Partial hash aggregates, so a Final or Complete one should not replace
          willNotWorkOnGpu("Replacing Final or Complete hash aggregates disabled")
         }
        case "final" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Complete)) {
          // replacing only Final hash aggregates, so a Partial or Complete one should not replace
          willNotWorkOnGpu("Replacing Partial or Complete hash aggregates disabled")
        }
        case "complete" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Final)) {
          // replacing only Complete hash aggregates, so a Partial or Final one should not replace
          willNotWorkOnGpu("Replacing Partial or Final hash aggregates disabled")
        }
        case _ =>
          throw new IllegalArgumentException(s"The hash aggregate replacement mode " +
            s"$hashAggReplaceMode is not valid. Valid options are: 'partial', " +
            s"'final', 'complete', or 'all'")
      }
    }
    if (!conf.partialMergeDistinctEnabled && hashAggMode.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
        s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
    if (agg.aggregateExpressions.exists(expr => expr.isDistinct)
      && agg.aggregateExpressions.exists(expr => expr.filter.isDefined)) {
      // Distinct with Filter is not supported on the CPU currently,
      // this makes sure that if we end up here, the plan falls back to th CPU,
      // which will do the right thing.
      willNotWorkOnGpu(
        "DISTINCT and FILTER cannot be used in aggregate functions at the same time")
    }
  }

        override def convertToGpu(): GpuExec = {
          GpuHashAggregateExec(
            requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
            groupingExpressions.map(_.convertToGpu()),
            aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
            aggregateAttributes.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
            agg.initialInputBufferOffset,
            resultExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
            childPlans(0).convertIfNeeded())
        }
      }),
    exec[SortAggregateExec](
      "The backend for sort based aggregations",
      (agg, conf, p, r) => new GpuSortAggregateMeta(agg, conf, p, r) {
        private val requiredChildDistributionExpressions: Option[Seq[BaseExprMeta[_]]] =
          agg.requiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
        private val groupingExpressions: Seq[BaseExprMeta[_]] =
          agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val aggregateExpressions: Seq[BaseExprMeta[_]] =
          agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val aggregateAttributes: Seq[BaseExprMeta[_]] =
          agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val resultExpressions: Seq[BaseExprMeta[_]] =
          agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childPlans: Seq[SparkPlanMeta[_]] =
          agg.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] =
          requiredChildDistributionExpressions.getOrElse(Seq.empty) ++
            groupingExpressions ++
            aggregateExpressions ++
            aggregateAttributes ++
            resultExpressions

  override def tagPlanForGpu(): Unit = {
    if (agg.groupingExpressions.isEmpty) {
      // first/last reductions not supported yet
      if (agg.aggregateExpressions.exists(e => e.aggregateFunction.isInstanceOf[First] ||
        e.aggregateFunction.isInstanceOf[Last])) {
        willNotWorkOnGpu("First/Last reductions are not supported on GPU")
      }
    }
    if (RapidsMeta.isAnyStringLit(agg.groupingExpressions)) {
      willNotWorkOnGpu("string literal values are not supported in a hash aggregate")
    }
    val groupingExpressionTypes = agg.groupingExpressions.map(_.dataType)
    if (conf.hasNans &&
      (groupingExpressionTypes.contains(FloatType) ||
        groupingExpressionTypes.contains(DoubleType))) {
      willNotWorkOnGpu("grouping expressions over floating point columns " +
        "that may contain -0.0 and NaN are disabled. You can bypass this by setting " +
        s"${RapidsConf.HAS_NANS}=false")
    }
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }
    val hashAggMode = agg.aggregateExpressions.map(_.mode).distinct
    val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
    if (!hashAggReplaceMode.equals("all")) {
      hashAggReplaceMode match {
        case "partial" => if (hashAggMode.contains(Final) || hashAggMode.contains(Complete)) {
          // replacing only Partial hash aggregates, so a Final or Commplete one should not replace
          willNotWorkOnGpu("Replacing Final or Complete hash aggregates disabled")
         }
        case "final" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Complete)) {
          // replacing only Final hash aggregates, so a Partial or Complete one should not replace
          willNotWorkOnGpu("Replacing Partial or Complete hash aggregates disabled")
        }
        case "complete" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Final)) {
          // replacing only Complete hash aggregates, so a Partial or Final one should not replace
          willNotWorkOnGpu("Replacing Partial or Final hash aggregates disabled")
        }
        case _ =>
          throw new IllegalArgumentException(s"The hash aggregate replacement mode " +
            s"${hashAggReplaceMode} is not valid. Valid options are: 'partial', 'final', " +
            s"'complete', or 'all'")
      }
    }
  }

  override def convertToGpu(): GpuExec = {
    // we simply convert to a HashAggregateExec and let GpuOverrides take care of inserting a
    // GpuSortExec if one is needed
    GpuHashAggregateExec(
      requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
      groupingExpressions.map(_.convertToGpu()),
      aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
      aggregateAttributes.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      agg.initialInputBufferOffset,
      resultExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      childPlans(0).convertIfNeeded())
  }

      }),
    exec[SortExec](
      "The backend for the sort operator",
      (sort, conf, p, r) => new GpuSortMeta(sort, conf, p, r) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          sort.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          sort.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    exec[ExpandExec](
      "The backend for the expand operator",
      (expand, conf, p, r) => new SparkPlanMeta(expand, conf, p, r) {
        private val gpuProjections: Seq[Seq[BaseExprMeta[_]]] =
          expand.projections.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))

        private val outputAttributes: Seq[BaseExprMeta[_]] =
          expand.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childPlans: Seq[SparkPlanMeta[_]] =
          expand.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = gpuProjections.flatten ++ outputAttributes

        /**
         * Convert what this wraps to a GPU enabled version.
         */
        override def convertToGpu(): GpuExec = {
          val projections = gpuProjections.map(_.map(_.convertToGpu()))
          GpuExpandExec(projections, expand.output,
            childPlans.head.convertIfNeeded())
        }
      }),
    exec[WindowExec](
      "Window-operator backend",
      (windowOp, conf, p, r) =>
        new GpuWindowExecMeta(windowOp, conf, p, r) {
  override val childPlans: Seq[SparkPlanMeta[_]] =
    windowOp.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] =
    windowOp.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  /**
   * Fetches WindowExpressions in input `windowExec`, via reflection.
   * As a byproduct, determines whether to return the original input columns,
   * as part of the output.
   *
   * (Spark versions that use `projectList` expect result columns
   * *not* to include the input columns.
   * Apache Spark expects the input columns, before the aggregation output columns.)
   *
   * @return WindowExpressions within windowExec,
   *         and a boolean, indicating the result column semantics
   *         (i.e. whether result columns should be returned *without* including the
   *         input columns).
   */
  def getWindowExpression: (Seq[NamedExpression], Boolean) = {
    var resultColumnsOnly : Boolean = false
    val expr = try {
      val resultMethod = windowOp.getClass.getMethod("windowExpression")
      resultMethod.invoke(windowOp).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case e: NoSuchMethodException =>
        resultColumnsOnly = true
        val winExpr = windowOp.getClass.getMethod("projectList")
        winExpr.invoke(windowOp).asInstanceOf[Seq[NamedExpression]]
    }
    (expr, resultColumnsOnly)
  }

  private val (inputWindowExpressions, resultColumnsOnly) = getWindowExpression


          val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
            inputWindowExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {

    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
        "cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))

  }

  override def convertToGpu(): GpuExec = {
    GpuWindowExec(
      windowExpressions.map(_.convertToGpu()),
      childPlans.head.convertIfNeeded(),
      resultColumnsOnly
    )
  }
        }
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
}

case class GpuOverrides() extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan) :SparkPlan = {
    val conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      val wrap = GpuOverrides.wrapPlan(plan, conf, None)
      wrap.tagForGpu()
      wrap.runAfterTagRules()
      val exp = conf.explain
      if (!exp.equalsIgnoreCase("NONE")) {
        logWarning(s"\n${wrap.explain(exp.equalsIgnoreCase("ALL"))}")
      }
      val convertedPlan = wrap.convertIfNeeded()
      addSortsIfNeeded(convertedPlan, conf)
    } else {
      plan
    }
  }

  private final class SortConfKeysAndIncompat extends ConfKeysAndIncompat {
    override val operationName: String = "Exec"
    override def confKey = "spark.rapids.sql.exec.SortExec"
  }

  // copied from Spark EnsureRequirements but only does the ordering checks and
  // check to convert any SortExec added to GpuSortExec
  private def ensureOrdering(operator: SparkPlan, conf: RapidsConf): SparkPlan = {
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildOrderings.length == children.length)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        val sort = SortExec(requiredOrdering, global = false, child = child)
        // just specifically check Sort to see if we can change Sort to GPUSort
        val sortMeta = new GpuSortMeta(sort, conf, None, new SortConfKeysAndIncompat) {
        override val childPlans: Seq[SparkPlanMeta[_]] =
          sort.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          sort.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        }
        sortMeta.initReasons()
        sortMeta.tagPlanForGpu()
        if (sortMeta.canThisBeReplaced) {
          sortMeta.convertToGpu()
        } else {
          sort
        }
      }
    }
    operator.withNewChildren(children)
  }

  def addSortsIfNeeded(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case operator: SparkPlan =>
        ensureOrdering(operator, conf)
    }
  }
}
