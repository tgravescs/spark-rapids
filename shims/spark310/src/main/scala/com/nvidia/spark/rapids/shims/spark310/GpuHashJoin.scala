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
package com.nvidia.spark.rapids.shims.spark310

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.joins.HashJoin
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuHashJoin {
  def tagJoin(
      meta: RapidsMeta[_, _, _],
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression]): Unit = joinType match {
    case _: InnerLike =>
    case FullOuter | RightOuter | LeftOuter | LeftSemi | LeftAnti =>
      if (condition.isDefined) {
        meta.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
      }
    case _ => meta.willNotWorkOnGpu(s"$joinType currently is not supported")
  }
}

trait GpuHashJoin extends GpuExec with HashJoin {

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }
  }

  protected lazy val (gpuBuildKeys, gpuStreamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  /**
   * Place the columns in left and the columns in right into a single ColumnarBatch
   */
  def combine(left: ColumnarBatch, right: ColumnarBatch): ColumnarBatch = {
    val l = GpuColumnVector.extractColumns(left)
    val r = GpuColumnVector.extractColumns(right)
    val c = l ++ r
    new ColumnarBatch(c.asInstanceOf[Array[ColumnVector]], left.numRows())
  }

  // TODO eventually dedupe the keys
  lazy val joinKeyIndices: Range = gpuBuildKeys.indices

  val localBuildOutput: Seq[Attribute] = buildPlan.output
  // The first columns are the ones we joined on and need to remove
  lazy val joinIndices: Seq[Int] = joinType match {
    case RightOuter =>
      // The left table and right table are switched in the output
      // because we don't support a right join, only left
      val numRight = right.output.length
      val numLeft = left.output.length
      val joinLength = joinKeyIndices.length
      def remap(index: Int): Int = {
        if (index < numLeft) {
          // part of the left table, but is on the right side of the tmp output
          index + joinLength + numRight
        } else {
          // part of the right table, but is on the left side of the tmp output
          index + joinLength - numLeft
        }
      }
      output.indices.map (remap)
    case _ =>
      val joinLength = joinKeyIndices.length
      output.indices.map (v => v + joinLength)
  }

  def doJoin(builtTable: Table,
      stream: Iterator[ColumnarBatch],
      boundCondition: Option[Expression],
      numOutputRows: SQLMetric,
      joinOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      joinTime: SQLMetric,
      filterTime: SQLMetric,
      totalTime: SQLMetric): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      import scala.collection.JavaConverters._
      var nextCb: Option[ColumnarBatch] = None
      var first: Boolean = true

      TaskContext.get().addTaskCompletionListener[Unit](_ => closeCb())

      def closeCb(): Unit = {
        nextCb.foreach(_.close())
        nextCb = None
      }

      override def hasNext: Boolean = {
        while (nextCb.isEmpty && (first || stream.hasNext)) {
          if (stream.hasNext) {
            val cb = stream.next()
            val startTime = System.nanoTime()
            nextCb = doJoin(builtTable, cb, boundCondition, joinOutputRows, numOutputRows,
              numOutputBatches, joinTime, filterTime)
            totalTime += (System.nanoTime() - startTime)
          } else if (first) {
            // We have to at least try one in some cases
            val startTime = System.nanoTime()
            val cb = GpuColumnVector.emptyBatch(streamedPlan.output.asJava)
            nextCb = doJoin(builtTable, cb, boundCondition, joinOutputRows, numOutputRows,
              numOutputBatches, joinTime, filterTime)
            totalTime += (System.nanoTime() - startTime)
          }
          first = false
        }
        nextCb.isDefined
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        val ret = nextCb.get
        nextCb = None
        ret
      }
    }
  }

  private[this] def doJoin(builtTable: Table,
      streamedBatch: ColumnarBatch,
      boundCondition: Option[Expression],
      numOutputRows: SQLMetric,
      numJoinOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      joinTime: SQLMetric,
      filterTime: SQLMetric): Option[ColumnarBatch] = {

    val streamedTable = try {
      val streamedKeysBatch = GpuProjectExec.project(streamedBatch, gpuStreamedKeys)
      try {
        val combined = combine(streamedKeysBatch, streamedBatch)
        GpuColumnVector.from(combined)
      } finally {
        streamedKeysBatch.close()
      }
    } finally {
      streamedBatch.close()
    }

    val nvtxRange = new NvtxWithMetrics("hash join", NvtxColor.ORANGE, joinTime)
    val joined = try {
      buildSide match {
        case BuildLeft => doJoinLeftRight(builtTable, streamedTable)
        case BuildRight => doJoinLeftRight(streamedTable, builtTable)
      }
    } finally {
      streamedTable.close()
      nvtxRange.close()
    }

    numJoinOutputRows += joined.numRows()

    val tmp = if (boundCondition.isDefined) {
      GpuFilter(joined, boundCondition.get, numOutputRows, numOutputBatches, filterTime)
    } else {
      numOutputRows += joined.numRows()
      numOutputBatches += 1
      joined
    }
    if (tmp.numRows() == 0) {
      // Not sure if there is a better way to work around this
      numOutputBatches.set(numOutputBatches.value - 1)
      tmp.close()
      None
    } else {
      Some(tmp)
    }
  }

  private[this] def doJoinLeftRight(leftTable: Table, rightTable: Table): ColumnarBatch = {
    val joinedTable = joinType match {
      case LeftOuter => leftTable.onColumns(joinKeyIndices: _*)
          .leftJoin(rightTable.onColumns(joinKeyIndices: _*), false)
      case RightOuter => rightTable.onColumns(joinKeyIndices: _*)
          .leftJoin(leftTable.onColumns(joinKeyIndices: _*), false)
      case _: InnerLike => leftTable.onColumns(joinKeyIndices: _*)
          .innerJoin(rightTable.onColumns(joinKeyIndices: _*), false)
      case LeftSemi => leftTable.onColumns(joinKeyIndices: _*)
          .leftSemiJoin(rightTable.onColumns(joinKeyIndices: _*), false)
      case LeftAnti => leftTable.onColumns(joinKeyIndices: _*)
          .leftAntiJoin(rightTable.onColumns(joinKeyIndices: _*), false)
      case FullOuter => leftTable.onColumns(joinKeyIndices: _*)
          .fullJoin(rightTable.onColumns(joinKeyIndices: _*), false)
      case _ => throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
          s" supported")
    }
    try {
      val result = joinIndices.map(joinIndex =>
        GpuColumnVector.from(joinedTable.getColumn(joinIndex).incRefCount()))
        .toArray[ColumnVector]

      new ColumnarBatch(result, joinedTable.getRowCount.toInt)
    } finally {
      joinedTable.close()
    }
  }
}
