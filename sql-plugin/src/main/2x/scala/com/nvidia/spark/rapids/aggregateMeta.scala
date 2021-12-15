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

import scala.annotation.tailrec
import scala.collection.mutable

import ai.rapids.cudf.DType

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.rapids.{CpuToGpuAggregateBufferConverter, GpuToCpuAggregateBufferConverter}
import org.apache.spark.sql.rapids.execution.{GpuShuffleMeta, TrampolineUtil}
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType}

abstract class GpuBaseAggregateMeta[INPUT <: SparkPlan](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule) extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

  // TODO no base in 2.x, need to use specific hashagg, etc..
  // TODO - support other agg types
  val agg: HashAggregateExec

  val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

  override def tagPlanForGpu(): Unit = {
    // We don't support Arrays and Maps as GroupBy keys yet, even they are nested in Structs. So,
    // we need to run recursive type check on the structs.
    val arrayOrMapGroupings = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType]))
    if (arrayOrMapGroupings) {
      willNotWorkOnGpu("ArrayTypes or MapTypes in grouping expressions are not supported")
    }

    val dec128Grouping = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[DecimalType] &&
            dt.asInstanceOf[DecimalType].precision > DType.DECIMAL64_MAX_PRECISION))
    if (dec128Grouping) {
      willNotWorkOnGpu("grouping by a 128-bit decimal value is not currently supported")
    }

    tagForReplaceMode()

    // no filter in 2.x
    /*
    if (agg.aggregateExpressions.exists(expr => expr.isDistinct)
        && agg.aggregateExpressions.exists(expr => expr.filter.isDefined)) {
      // Distinct with Filter is not supported on the GPU currently,
      // This makes sure that if we end up here, the plan falls back to the CPU
      // which will do the right thing.
      willNotWorkOnGpu(
        "DISTINCT and FILTER cannot be used in aggregate functions at the same time")
    }

     */
  }

  /**
   * Tagging checks tied to configs that control the aggregation modes that are replaced.
   *
   * The rule of replacement is determined by `spark.rapids.sql.hashAgg.replaceMode`, which
   * is a string configuration consisting of AggregateMode names in lower cases connected by
   * &(AND) and |(OR). The default value of this config is `all`, which indicates replacing all
   * aggregates if possible.
   *
   * The `|` serves as the outer connector, which represents patterns of both sides are able to be
   * replaced. For instance, `final|partialMerge` indicates that aggregate plans purely in either
   * Final mode or PartialMerge mode can be replaced. But aggregate plans also contain
   * AggExpressions of other mode will NOT be replaced, such as: stage 3 of single distinct
   * aggregate who contains both Partial and PartialMerge.
   *
   * On the contrary, the `&` serves as the inner connector, which intersects modes of both sides
   * to form a mode pattern. The replacement only takes place for aggregate plans who have the
   * exact same mode pattern as what defined the rule. For instance, `partial&partialMerge` means
   * that aggregate plans can be only replaced if they contain AggExpressions of Partial and
   * contain AggExpressions of PartialMerge and don't contain AggExpressions of other modes.
   *
   * In practice, we need to combine `|` and `&` to form some sophisticated patterns. For instance,
   * `final&complete|final|partialMerge` represents aggregate plans in three different patterns are
   * GPU-replaceable: plans contain both Final and Complete modes; plans only contain Final mode;
   * plans only contain PartialMerge mode.
   */
  private def tagForReplaceMode(): Unit = {
    val aggPattern = agg.aggregateExpressions.map(_.mode).toSet
    val strPatternToReplace = conf.hashAggReplaceMode.toLowerCase

    if (aggPattern.nonEmpty && strPatternToReplace != "all") {
      val aggPatternsCanReplace = strPatternToReplace.split("\\|").map { subPattern =>
        subPattern.split("&").map {
          case "partial" => Partial
          case "partialmerge" => PartialMerge
          case "final" => Final
          case "complete" => Complete
          case s => throw new IllegalArgumentException(s"Invalid Aggregate Mode $s")
        }.toSet
      }
      if (!aggPatternsCanReplace.contains(aggPattern)) {
        val message = aggPattern.map(_.toString).mkString(",")
        willNotWorkOnGpu(s"Replacing mode pattern `$message` hash aggregates disabled")
      } else if (aggPattern == Set(Partial)) {
        // In partial mode, if there are non-distinct functions and multiple distinct functions,
        // non-distinct functions are computed using the First operator. The final result would be
        // incorrect for non-distinct functions for partition size > 1. Reason for this is - if
        // the first batch computed and sent to CPU doesn't contain all the rows required to
        // compute non-distinct function(s), then Spark would consider that value as final result
        // (due to First). Fall back to CPU in this case.
        if (AggregateUtils.shouldFallbackMultiDistinct(agg.aggregateExpressions)) {
          willNotWorkOnGpu("Aggregates of non-distinct functions with multiple distinct " +
              "functions are non-deterministic for non-distinct functions as it is " +
              "computed using First.")
        }
      }
    }

    if (!conf.partialMergeDistinctEnabled && aggPattern.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
          s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
  }

  /*
  override def convertToGpu(): GpuExec = {
    GpuHashAggregateExec(
      aggRequiredChildDistributionExpressions,
      groupingExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
      aggregateExpressions.map(_.convertToGpu().asInstanceOf[GpuAggregateExpression]),
      aggregateAttributes.map(_.convertToGpu().asInstanceOf[Attribute]),
      resultExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
      childPlans.head.convertIfNeeded(),
      conf.gpuTargetBatchSizeBytes)
  }
  */
}

/**
 * Base class for metadata around `SortAggregateExec` and `ObjectHashAggregateExec`, which may
 * contain TypedImperativeAggregate functions in aggregate expressions.
 */
abstract class GpuTypedImperativeSupportedAggregateExecMeta[INPUT <: HashAggregateExec](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule) extends GpuBaseAggregateMeta[INPUT](plan,
  aggRequiredChildDistributionExpressions, conf, parent, rule) {

  private val mayNeedAggBufferConversion: Boolean =
    agg.aggregateExpressions.exists { expr =>
      expr.aggregateFunction.isInstanceOf[TypedImperativeAggregate[_]] &&
          (expr.mode == Partial || expr.mode == PartialMerge)
    }

  // overriding data types of Aggregation Buffers if necessary
  if (mayNeedAggBufferConversion) overrideAggBufTypes()

  override protected lazy val outputTypeMetas: Option[Seq[DataTypeMeta]] =
    if (mayNeedAggBufferConversion) {
      Some(resultExpressions.map(_.typeMeta))
    } else {
      None
    }

  override val availableRuntimeDataTransition: Boolean =
    aggregateExpressions.map(_.childExprs.head).forall {
      case aggMeta: TypedImperativeAggExprMeta[_] => aggMeta.supportBufferConversion
      case _ => true
    }

  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    // If a typedImperativeAggregate function run across CPU and GPU (ex: Partial mode on CPU,
    // Merge mode on GPU), it will lead to a runtime crash. Because aggregation buffers produced
    // by the previous stage of function are NOT in the same format as the later stage consuming.
    // If buffer converters are available for all incompatible buffers, we will build these buffer
    // converters and bind them to certain plans, in order to integrate these converters into
    // R2C/C2R Transitions during GpuTransitionOverrides to fix the gap between GPU and CPU.
    // Otherwise, we have to fall back all Aggregate stages to CPU once any of them did fallback.
    //
    // The binding also works when AQE is on, since it leverages the TreeNodeTag to cache buffer
    // converters.
    // TODO - nee d logical plan for 2.x
    // GpuTypedImperativeSupportedAggregateExecMeta.handleAggregationBuffer(this)
  }

  /*
  override def convertToGpu(): GpuExec = {
    if (mayNeedAggBufferConversion) {
      // transforms the data types of aggregate attributes with typeMeta
      val aggAttributes = aggregateAttributes.map {
        case meta if meta.typeMeta.typeConverted =>
          val ref = meta.wrapped.asInstanceOf[AttributeReference]
          val converted = ref.copy(
            dataType = meta.typeMeta.dataType.get)(ref.exprId, ref.qualifier)
          GpuOverrides.wrapExpr(converted, conf, Some(this))
        case meta => meta
      }
      // transforms the data types of result expressions with typeMeta
      val retExpressions = resultExpressions.map {
        case meta if meta.typeMeta.typeConverted =>
          val ref = meta.wrapped.asInstanceOf[AttributeReference]
          val converted = ref.copy(
            dataType = meta.typeMeta.dataType.get)(ref.exprId, ref.qualifier)
          GpuOverrides.wrapExpr(converted, conf, Some(this))
        case meta => meta
      }
      GpuHashAggregateExec(
        aggRequiredChildDistributionExpressions,
        groupingExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        aggregateExpressions.map(_.convertToGpu().asInstanceOf[GpuAggregateExpression]),
        aggAttributes.map(_.convertToGpu().asInstanceOf[Attribute]),
        retExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        childPlans.head.convertIfNeeded(),
        conf.gpuTargetBatchSizeBytes)
    } else {
      super.convertToGpu()
    }
  }

    */
  /**
   * The method replaces data types of aggregation buffers created by TypedImperativeAggregate
   * functions with the actual data types used in the GPU runtime.
   *
   * Firstly, this method traverses aggregateFunctions, to search attributes referring to
   * aggregation buffers of TypedImperativeAggregate functions.
   * Then, we extract the desired (actual) data types on GPU runtime for these attributes,
   * and map them to expression IDs of attributes.
   * At last, we traverse aggregateAttributes and resultExpressions, overriding data type in
   * RapidsMeta if necessary, in order to ensure TypeChecks tagging exact data types in runtime.
   */
  private def overrideAggBufTypes(): Unit = {
    val desiredAggBufTypes = mutable.HashMap.empty[ExprId, DataType]
    val desiredInputAggBufTypes = mutable.HashMap.empty[ExprId, DataType]
    // Collects exprId from TypedImperativeAggBufferAttributes, and maps them to the data type
    // of `TypedImperativeAggExprMeta.aggBufferAttribute`.
    aggregateExpressions.map(_.childExprs.head).foreach {
      case aggMeta: TypedImperativeAggExprMeta[_] =>
        val aggFn = aggMeta.wrapped.asInstanceOf[TypedImperativeAggregate[_]]
        val desiredType = aggMeta.aggBufferAttribute.dataType

        desiredAggBufTypes(aggFn.aggBufferAttributes.head.exprId) = desiredType
        desiredInputAggBufTypes(aggFn.inputAggBufferAttributes.head.exprId) = desiredType

      case _ =>
    }

    // Overrides the data types of typed imperative aggregation buffers for type checking
    aggregateAttributes.foreach { attrMeta =>
      attrMeta.wrapped match {
        case ar: AttributeReference if desiredAggBufTypes.contains(ar.exprId) =>
          attrMeta.overrideDataType(desiredAggBufTypes(ar.exprId))
        case _ =>
      }
    }
    resultExpressions.foreach { retMeta =>
      retMeta.wrapped match {
        case ar: AttributeReference if desiredInputAggBufTypes.contains(ar.exprId) =>
          retMeta.overrideDataType(desiredInputAggBufTypes(ar.exprId))
        case _ =>
      }
    }
  }
}

object GpuTypedImperativeSupportedAggregateExecMeta {

 /* private val bufferConverterInjected = TreeNodeTag[Boolean](
    "rapids.gpu.bufferConverterInjected")

  */

  /**
   * The method will bind buffer converters (CPU Expressions) to certain CPU Plans if necessary,
   * so the method returns nothing. The binding work will help us to insert these converters as
   * pre/post processing of GpuRowToColumnarExec/GpuColumnarToRowExec during the materialization
   * of these transition Plans.
   *
   * In the beginning, it collects all physical Aggregate Plans (stages) of current Aggregate.
   * Then, it goes through all stages in pair, to check whether buffer converters need to be
   * inserted between its child and itself.
   * If it is necessary, it goes on to check whether all these buffers are available to convert.
   * If not, it falls back all stages to CPU to keep consistency of intermediate data; Otherwise,
   * it creates buffer converters based on the createBufferConverter methods of
   * TypedImperativeAggExprMeta, and binds these newly-created converters to certain Plans.
   *
   * In terms of binding, it is critical and sophisticated to find out where to bind these
   * converters. Generally, we need to find out CPU plans right before/after the potential R2C/C2R
   * transitions, so as to integrate these converters during post columnar overriding. These plans
   * may be Aggregate stages themselves. They may also be plans inserted between Aggregate stages,
   * such as: ShuffleExchangeExec, SortExec.
   *
   * The binding carries out by storing buffer converters as tag values of certain CPU Plans. These
   * plans are either the child of GpuRowToColumnarExec or the parent of GpuColumnarToRowExec. The
   * converters are cached inside CPU Plans rather than GPU ones (like: the child of
   * GpuColumnarToRowExec), because we can't access the GPU Plans during binding since they are
   * yet created. And GPU Plans created by RapidsMeta don't keep the tags of their CPU
   * counterparts.
   */
    /*
  private def handleAggregationBuffer(
      meta: GpuTypedImperativeSupportedAggregateExecMeta[_]): Unit = {
    // We only run the check for final stages which contain TypedImperativeAggregate.
    val needToCheck = containTypedImperativeAggregate(meta, Some(Final))
    if (!needToCheck) return
    // Avoid duplicated check and fallback.
   /* val checked = meta.agg.getTagValue[Boolean](bufferConverterInjected).contains(true)
    if (checked) return
    meta.agg.setTagValue(bufferConverterInjected, true)

    */

    // TODO - SparkPlan doesn't have logiclink in 2.x
    // Fetch AggregateMetas of all stages which belong to current Aggregate
    val stages = getAggregateOfAllStages(meta, meta.agg.logicalLink.get)

    // Find out stages in which the buffer converters are essential.
    val needBufferConversion = stages.indices.map {
      case i if i == stages.length - 1 => false
      case i =>
        // Buffer converters are only needed to inject between two stages if [A] and [B].
        // [A]. there will be a R2C or C2R transition between them
        // [B]. there exists TypedImperativeAggregate functions in each of them
        (stages(i).canThisBeReplaced ^ stages(i + 1).canThisBeReplaced) &&
            containTypedImperativeAggregate(stages(i)) &&
            containTypedImperativeAggregate(stages(i + 1))
    }

    // Return if all internal aggregation buffers are compatible with GPU Overrides.
    if (needBufferConversion.forall(!_)) return

    // Fall back all GPU supported stages to CPU, if there exists TypedImperativeAggregate buffer
    // who doesn't support data format transition in runtime. Otherwise, build buffer converters
    // and bind them to certain SparkPlans.
    val entireFallback = !stages.zip(needBufferConversion).forall { case (stage, needConvert) =>
      !needConvert || stage.availableRuntimeDataTransition
    }
    if (entireFallback) {
      stages.foreach {
        case aggMeta if aggMeta.canThisBeReplaced =>
          aggMeta.willNotWorkOnGpu("Associated fallback for TypedImperativeAggregate")
        case _ =>
      }
    } else {
      bindBufferConverters(stages, needBufferConversion)
    }
  }

     */

  /**
   * Bind converters as TreeNodeTags into the CPU plans who are right before/after the potential
   * R2C/C2R transitions (the transitions are yet inserted).
   *
   * These converters are CPU expressions, and they will be integrated into GpuRowToColumnarExec/
   * GpuColumnarToRowExec as pre/post transitions. What we do here, is to create these converters
   * according to the stage pairs.
   */
  private def bindBufferConverters(stages: Seq[GpuBaseAggregateMeta[_]],
      needBufferConversion: Seq[Boolean]): Unit = {

    needBufferConversion.zipWithIndex.foreach {
      case (needConvert, i) if needConvert =>
        // Find the next edge from the given stage which is a splitting point between CPU plans
        // and GPU plans. The method returns a pair of plans around the edge. For instance,
        //
        // stage(i): GpuObjectHashAggregateExec ->
        //    pair_head: GpuShuffleExec ->
        //        pair_tail and stage(i + 1): ObjectHashAggregateExec
        //
        // In example above, stage(i) is a GpuPlan while stage(i + 1) is not. And we assume
        // both of them including TypedImperativeAgg. So, in terms of stage(i) the buffer
        // conversion is necessary. Then, we call the method nextEdgeForConversion to find
        // the edge next to stage(i), which is between GpuShuffleExec and stage(i + 1).
        nextEdgeForConversion(stages(i)) match {
          // create preRowToColumnarTransition, and bind it to the child node (CPU plan) of
          // GpuRowToColumnarExec
          case List(parent, child) if parent.canThisBeReplaced =>
            val childPlan = child.wrapped.asInstanceOf[SparkPlan]
            val expressions = createBufferConverter(stages(i), stages(i + 1), true)
            // childPlan.setTagValue(GpuOverrides.preRowToColProjection, expressions)
          // create postColumnarToRowTransition, and bind it to the parent node (CPU plan) of
          // GpuColumnarToRowExec
          case List(parent, _) =>
            val parentPlan = parent.wrapped.asInstanceOf[SparkPlan]
            val expressions = createBufferConverter(stages(i), stages(i + 1), false)
            // parentPlan.setTagValue(GpuOverrides.postColToRowProjection, expressions)
        }
      case _ =>
    }
  }

  private def containTypedImperativeAggregate(meta: GpuBaseAggregateMeta[_],
      desiredMode: Option[AggregateMode] = None): Boolean =
    meta.agg.aggregateExpressions.exists {
      case e: AggregateExpression if desiredMode.forall(_ == e.mode) =>
        e.aggregateFunction.isInstanceOf[TypedImperativeAggregate[_]]
      case _ => false
    }

  private def createBufferConverter(mergeAggMeta: GpuBaseAggregateMeta[_],
      partialAggMeta: GpuBaseAggregateMeta[_],
      fromCpuToGpu: Boolean): Seq[NamedExpression] = {

    val converters = mutable.Queue[Either[
        CpuToGpuAggregateBufferConverter, GpuToCpuAggregateBufferConverter]]()
    mergeAggMeta.childExprs.foreach {
      case e if e.childExprs.length == 1 &&
          e.childExprs.head.isInstanceOf[TypedImperativeAggExprMeta[_]] =>
        e.wrapped.asInstanceOf[AggregateExpression].mode match {
          case Final | PartialMerge =>
            val typImpAggMeta = e.childExprs.head.asInstanceOf[TypedImperativeAggExprMeta[_]]
            val converter = if (fromCpuToGpu) {
              Left(typImpAggMeta.createCpuToGpuBufferConverter())
            } else {
              Right(typImpAggMeta.createGpuToCpuBufferConverter())
            }
            converters.enqueue(converter)
          case _ =>
        }
      case _ =>
    }

    val expressions = partialAggMeta.resultExpressions.map {
      case retExpr if retExpr.typeMeta.typeConverted =>
        val resultExpr = retExpr.wrapped.asInstanceOf[AttributeReference]
        val ref = if (fromCpuToGpu) {
          resultExpr
        } else {
          resultExpr.copy(dataType = retExpr.typeMeta.dataType.get)(
            resultExpr.exprId, resultExpr.qualifier)
        }
        converters.dequeue() match {
          case Left(converter) =>
            ShimLoader.getSparkShims.alias(converter.createExpression(ref),
              ref.name + "_converted")(NamedExpression.newExprId)
          case Right(converter) =>
            ShimLoader.getSparkShims.alias(converter.createExpression(ref),
              ref.name + "_converted")(NamedExpression.newExprId)
        }
      case retExpr =>
        retExpr.wrapped.asInstanceOf[NamedExpression]
    }

    expressions
  }

  /*
  private def getAggregateOfAllStages(
      currentMeta: SparkPlanMeta[_], logical: LogicalPlan): List[GpuBaseAggregateMeta[_]] = {
    currentMeta match {
      case aggMeta: GpuBaseAggregateMeta[_] if aggMeta.agg.logicalLink.contains(logical) =>
        List[GpuBaseAggregateMeta[_]](aggMeta) ++
            getAggregateOfAllStages(aggMeta.childPlans.head, logical)
      case shuffleMeta: GpuShuffleMeta =>
        getAggregateOfAllStages(shuffleMeta.childPlans.head, logical)
      case sortMeta: GpuSortMeta =>
        getAggregateOfAllStages(sortMeta.childPlans.head, logical)
      case _ =>
        List[GpuBaseAggregateMeta[_]]()
    }
  }

   */

  @tailrec
  private def nextEdgeForConversion(meta: SparkPlanMeta[_]): Seq[SparkPlanMeta[_]] = {
    val child = meta.childPlans.head
    if (meta.canThisBeReplaced ^ child.canThisBeReplaced) {
      List(meta, child)
    } else {
      nextEdgeForConversion(child)
    }
  }
}

class GpuHashAggregateMeta(
    override val agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseAggregateMeta(agg, agg.requiredChildDistributionExpressions,
      conf, parent, rule)

/*
class GpuSortAggregateExecMeta(
    override val agg: SortAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
      agg.requiredChildDistributionExpressions, conf, parent, rule) {
  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    // Make sure this is the last check - if this is SortAggregate, the children can be sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // HashAggregate.  We don't want to do this if there is a first or last aggregate,
    // because dropping the sort will make them no longer deterministic.
    // In the future we might be able to pull the sort functionality into the aggregate so
    // we can sort a single batch at a time and sort the combined result as well which would help
    // with data skew.
    val hasFirstOrLast = agg.aggregateExpressions.exists { agg =>
      agg.aggregateFunction match {
        case _: First | _: Last => true
        case _ => false
      }
    }
    if (canThisBeReplaced && !hasFirstOrLast) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu("one of the preceding SortExec's cannot be replaced")
          } else {
            plan.shouldBeRemoved("replacing sort aggregate with hash aggregate")
          }
        }
      }
    }
  }
}

class GpuObjectHashAggregateExecMeta(
    override val agg: ObjectHashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
      agg.requiredChildDistributionExpressions, conf, parent, rule)
*/