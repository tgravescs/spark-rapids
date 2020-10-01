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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids.GpuMetricNames._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{CollectLimitExec, LimitExec, ShuffledRowRDD, SparkPlan, UnaryExecNode, UnsafeRowSerializer}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExec, ShuffledBatchRDD}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Helper trait which defines methods that are shared by both
 * [[GpuLocalLimitExec]] and [[GpuGlobalLimitExec]].
 */
trait GpuBaseLimitExec extends LimitExec with GpuExec with Logging {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    val part = child.outputPartitioning
    logWarning("base limit exe cpartitioing : " + part)
    return part
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    logWarning("base limit exec do execut")
    new Exception("Stack trace").printStackTrace();
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)

    logWarning("limit number rows is: " + limit)

    val crdd = child.executeColumnar()
    crdd.mapPartitions { cbIter =>
      new Iterator[ColumnarBatch] {
        var remainingLimit = limit

        override def hasNext: Boolean = remainingLimit > 0 && cbIter.hasNext

        override def next(): ColumnarBatch = {
          val batch = cbIter.next()
          withResource(new NvtxWithMetrics("limit", NvtxColor.ORANGE, totalTime)) { _ =>
            val result = if (batch.numRows() > remainingLimit) {
              sliceBatch(batch)
            } else {
              batch
            }
            numOutputBatches += 1
            numOutputRows += result.numRows()
            remainingLimit -= result.numRows()
            // logWarning("remaining limit number rows is: " + remainingLimit + " result rows: " + result.numRows())
            result
          }
        }

        def sliceBatch(batch: ColumnarBatch): ColumnarBatch = {
          val numColumns = batch.numCols()
          val resultCVs = new ArrayBuffer[GpuColumnVector](numColumns)
          var exception: Throwable = null
          var table: Table = null
          try {
            if (numColumns > 0) {
              table = GpuColumnVector.from(batch)
              (0 until numColumns).foreach(i => {
                val subVector = table.getColumn(i).subVector(0, remainingLimit)
                assert(subVector != null)
                resultCVs.append(GpuColumnVector.from(subVector))
                assert(subVector.getRowCount == remainingLimit,
                  s"result rowcount ${subVector.getRowCount} is not equal to the " +
                    s"remainingLimit $remainingLimit")
              })
            }
            new ColumnarBatch(resultCVs.toArray, remainingLimit)
          } catch {
            case e: Throwable => exception = e
              throw e
          } finally {
            if (exception != null) {
              resultCVs.foreach(gpuVector => gpuVector.safeClose(exception))
            }
            if (table != null) {
              table.safeClose(exception)
            }
            batch.safeClose(exception)
          }
        }
      }
    }
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class GpuLocalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec {
  override def outputPartitioning: Partitioning = {
    val part = child.outputPartitioning
    // new Exception("Stack trace").printStackTrace();
    // logWarning("local limit child is: " + child)
    logWarning("local limit partitioing : " + part)
    return part
  }
  override def executeCollect(): Array[InternalRow] = {
    logWarning("called execute collect in local limt")
    child.executeTake(limit)
  }
}

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GpuGlobalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec with Logging {
  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
  override def outputPartitioning: Partitioning = { 
    logWarning("global liiomit exec singlepartition")
    SinglePartition
  }
  override def executeCollect(): Array[InternalRow] = {
    logWarning("called execute collect in global limt")
    child.executeTake(limit)
  }
}

class GpuCollectLimitMeta(
                      collectLimit: CollectLimitExec,
                      conf: RapidsConf,
                      parent: Option[RapidsMeta[_, _, _]],
                      rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[CollectLimitExec](collectLimit, conf, parent, rule) with Logging {
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(collectLimit.outputPartitioning, conf, Some(this)))


  override def convertToGpu(): GpuExec = {
    logWarning("converting to gpu global imiit shuffle, local limit")
    // logWarning("convert limit child is: " + childPlans)
    // GpuGlobalLimitExec(collectLimit.limit,
     //  ShimLoader.getSparkShims.getGpuShuffleExchangeExec(GpuSinglePartitioning(Seq.empty),
        GpuLocalLimitExec(collectLimit.limit, childPlans(0).convertIfNeeded()) // ))
  }


}
