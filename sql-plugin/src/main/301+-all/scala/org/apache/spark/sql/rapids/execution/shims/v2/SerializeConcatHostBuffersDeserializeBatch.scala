/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import java.io._
import java.util.UUID
import java.util.concurrent._

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.control.NonFatal

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, NvtxColor, NvtxRange}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimUnaryExecNode

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

@SerialVersionUID(100L)
class SerializeConcatHostBuffersDeserializeBatch(
    private val data: Array[SerializeBatchDeserializeHostBuffer],
    private val output: Seq[Attribute])
  extends Serializable with Arm with AutoCloseable {
  @transient private val headers = data.map(_.header)
  @transient private val buffers = data.map(_.buffer)
  @transient private var batchInternal: ColumnarBatch = null

  def batch: ColumnarBatch = this.synchronized {
    if (batchInternal == null) {
      // TODO we should come up with a better way for this to happen directly...
      val out = new ByteArrayOutputStream()
      val oout = new ObjectOutputStream(out)
      writeObject(oout)
      val barr = out.toByteArray
      val oin = new ObjectInputStream(new ByteArrayInputStream(barr))
      readObject(oin)
    }
    batchInternal
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    if (headers.length == 0) {
      import scala.collection.JavaConverters._
      // We didn't get any data back, but we need to write out an empty table that matches
      withResource(GpuColumnVector.emptyHostColumns(output.asJava)) { hostVectors =>
        JCudfSerialization.writeToStream(hostVectors, out, 0, 0)
      }
      out.writeObject(output.map(_.dataType).toArray)
    } else if (headers.head.getNumColumns == 0) {
      JCudfSerialization.writeRowsToStream(out, numRows)
    } else {
      JCudfSerialization.writeConcatedStream(headers, buffers, out)
      out.writeObject(output.map(_.dataType).toArray)
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val range = new NvtxRange("DeserializeBatch", NvtxColor.PURPLE)
    try {
      val tableInfo: JCudfSerialization.TableAndRowCountPair =
        JCudfSerialization.readTableFrom(in)
      try {
        val table = tableInfo.getContiguousTable
        if (table == null) {
          val numRows = tableInfo.getNumRows
          this.batchInternal = new ColumnarBatch(new Array[ColumnVector](0), numRows)
        } else {
          val colDataTypes = in.readObject().asInstanceOf[Array[DataType]]
          this.batchInternal = GpuColumnVectorFromBuffer.from(table, colDataTypes)
          GpuColumnVector.extractBases(this.batchInternal).foreach(_.noWarnLeakExpected())
        }
      } finally {
        tableInfo.close()
      }
    } finally {
      range.close()
    }
  }

  def numRows: Int = {
    if (batchInternal != null) {
      batchInternal.numRows()
    } else {
      headers.map(_.getNumRows).sum
    }
  }

  def dataSize: Long = {
    if (batchInternal != null) {
      val bases = GpuColumnVector.extractBases(batchInternal).map(_.copyToHost())
      try {
        JCudfSerialization.getSerializedSizeInBytes(bases, 0, batchInternal.numRows())
      } finally {
        bases.safeClose()
      }
    } else {
      buffers.map(_.getLength).sum
    }
  }

  override def close(): Unit = {
    data.safeClose()
    if (batchInternal != null) {
      batchInternal.close()
    }
  }
}
