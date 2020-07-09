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

import ai.rapids.cudf.{ColumnVector, NvtxColor, Table}
import com.nvidia.spark.rapids.GpuMetricNames.{NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, TOTAL_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, CreateArray, Explode, Expression, Literal, PosExplode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.{GenerateExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * Takes the place of GenerateExec(PosExplode(CreateArray(_))).  It would be great to do it in a
 * more general case but because we don't support arrays/maps, we have to hard code the cases
 * where we don't actually need to put the data into an array first.
 */
case class GpuGenerateExec(
    includePos: Boolean,
    arrayProject: Seq[Expression],
    requiredChildOutput: Seq[Attribute],
    generatorOutput: Seq[Attribute],
    child: SparkPlan
) extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val boundArrayProjectList =
      GpuBindReferences.bindGpuReferences(arrayProject, child.output).toArray
    val numArrayColumns = boundArrayProjectList.length
    val boundOthersProjectList: Array[GpuExpression] =
      GpuBindReferences.bindGpuReferences(requiredChildOutput, child.output).toArray
    val numOtherColumns = boundOthersProjectList.length
    val numExplodeColumns = if (includePos) 2 else 1

    child.executeColumnar().mapPartitions { it =>
      new Iterator[ColumnarBatch] {
        var currentBatch: ColumnarBatch = _
        var indexIntoData = 0

        private def closeCurrent(): Unit = if (currentBatch != null) {
          currentBatch.close()
          currentBatch = null
        }

        TaskContext.get().addTaskCompletionListener[Unit](_ => closeCurrent())

        def fetchNextBatch(): Unit = {
          indexIntoData = 0
          closeCurrent()
          if (it.hasNext) {
            currentBatch = it.next()
          }
        }

        override def hasNext: Boolean = {
          if (currentBatch == null || indexIntoData >= numArrayColumns) {
            fetchNextBatch()
          }
          currentBatch != null
        }

        override def next(): ColumnarBatch = {
          if (currentBatch == null || indexIntoData >= numArrayColumns) {
            fetchNextBatch()
          }
          withResource(new NvtxWithMetrics("GpuGenerateExec", NvtxColor.PURPLE, totalTime)) { _ =>
            val result = new Array[ColumnVector](numExplodeColumns + numOtherColumns)
            try {
              withResource(GpuProjectExec.project(currentBatch, boundOthersProjectList)) { cb =>
                (0 until cb.numCols()).foreach { i =>
                  result(i) = cb.column(i).asInstanceOf[GpuColumnVector].getBase.incRefCount()
                }
              }
              if (includePos) {
                result(numOtherColumns) = withResource(GpuScalar.from(indexIntoData, IntegerType)) {
                  scalar => ColumnVector.fromScalar(scalar, currentBatch.numRows())
                }
              }
              result(numOtherColumns + numExplodeColumns - 1) =
                withResource(GpuProjectExec.project(currentBatch,
                  Seq(boundArrayProjectList(indexIntoData)))) { cb =>
                  cb.column(0).asInstanceOf[GpuColumnVector].getBase.incRefCount()
                }

              withResource(new Table(result: _*)) { table =>
                indexIntoData += 1
                numOutputBatches += 1
                numOutputRows += table.getRowCount
                GpuColumnVector.from(table)
              }
            } finally {
              result.safeClose()
            }
          }
        }
      }
    }
  }
}
