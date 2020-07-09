/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.GpuMetricNames._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

abstract class GpuWindowExecMeta(windowExec: WindowExec,
                        conf: RapidsConf,
                        parent: Option[RapidsMeta[_, _, _]],
                        rule: ConfKeysAndIncompat)
  extends WindowExecBaseMeta[WindowExec](windowExec, conf, parent, rule) {


}

case class GpuWindowExec(
    windowExpressionAliases: Seq[Expression],
    child: SparkPlan,
    resultColumnsOnly: Boolean
  ) extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = if (resultColumnsOnly) {
    windowExpressionAliases.map(_.asInstanceOf[NamedExpression].toAttribute)
  } else {
    child.output ++ windowExpressionAliases.map(_.asInstanceOf[NamedExpression].toAttribute)
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val additionalMetrics: Map[String, SQLMetric] =
    Map(
      NUM_INPUT_ROWS ->
        SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_ROWS),
      NUM_INPUT_BATCHES ->
        SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_BATCHES),
      PEAK_DEVICE_MEMORY ->
        SQLMetrics.createSizeMetric(sparkContext, DESCRIPTION_PEAK_DEVICE_MEMORY)
    )

  // Job metrics.
  private var maxDeviceMemory = 0L
  private val peakDeviceMemoryMetric = metrics(GpuMetricNames.PEAK_DEVICE_MEMORY)
  private val numInputBatchesMetric = metrics(GpuMetricNames.NUM_INPUT_BATCHES)
  private val numInputRowsMetric = metrics(GpuMetricNames.NUM_INPUT_ROWS)
  private val numOutputBatchesMetric = metrics(GpuMetricNames.NUM_OUTPUT_BATCHES)
  private val numOutputRowsMetric = metrics(GpuMetricNames.NUM_OUTPUT_ROWS)
  private val totalTimeMetric = metrics(GpuMetricNames.TOTAL_TIME)

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")

  private def bindReferences() : Seq[GpuExpression] = {

    // Address bindings for all expressions evaluated by WindowExec.
    val boundProjectList = windowExpressionAliases.map(
      alias => GpuBindReferences.bindReference(alias, child.output))

    // Bind aggregation column.
    boundProjectList.map(
      expr => expr.transform {
        case windowExpr: GpuWindowExpression =>
          val boundAggExpression = GpuBindReferences.bindReference(
            windowExpr.windowFunction match {
              case aggExpression: GpuAggregateExpression =>
                aggExpression.aggregateFunction.inputProjection.head
              case _ : GpuRowNumber => GpuLiteral(1, IntegerType)
              case anythingElse =>
                throw new IllegalStateException(s"Unexpected window operation " +
                  s"${anythingElse.prettyName}")
            },
            child.output)
          windowExpr.setBoundAggCol(boundAggExpression)
          windowExpr
      }.asInstanceOf[GpuExpression]
    )
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val boundOutputProjectList = bindReferences()

    val input = child.executeColumnar()
    input.map {
      cb => {

        numInputBatchesMetric += 1
        numInputRowsMetric += cb.numRows

        var originalCols: Array[GpuColumnVector] = null
        var aggCols     : Array[GpuColumnVector] = null

        try {
          originalCols = GpuColumnVector.extractColumns(cb)

          withResource(
            new NvtxWithMetrics(
              "WindowExec projections", NvtxColor.GREEN, totalTimeMetric)
            ) { _ =>
                aggCols = boundOutputProjectList.map(
                  _.columnarEval(cb).asInstanceOf[GpuColumnVector]).toArray
            }

          numOutputBatchesMetric += 1
          numOutputRowsMetric += cb.numRows

          val outputBatch = if (resultColumnsOnly) {
            new ColumnarBatch(aggCols.asInstanceOf[Array[ColumnVector]], cb.numRows())
          } else {
            originalCols.foreach(_.incRefCount())
            new ColumnarBatch(originalCols ++ aggCols, cb.numRows())
          }

          maxDeviceMemory = maxDeviceMemory.max(
            GpuColumnVector.getTotalDeviceMemoryUsed(outputBatch))
          peakDeviceMemoryMetric.set(maxDeviceMemory)

          outputBatch
        } finally {
          cb.close()
        }
      }
    }
  }
}
