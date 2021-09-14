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
class SerializeBatchDeserializeHostBuffer(batch: ColumnarBatch)
  extends Serializable with AutoCloseable {
  @transient private var columns = GpuColumnVector.extractBases(batch).map(_.copyToHost())
  @transient var header: JCudfSerialization.SerializedTableHeader = null
  @transient var buffer: HostMemoryBuffer = null
  @transient private val numRows = batch.numRows()

  private def writeObject(out: ObjectOutputStream): Unit = {
    val range = new NvtxRange("SerializeBatch", NvtxColor.PURPLE)
    try {
      if (buffer != null) {
        throw new IllegalStateException("Cannot re-serialize a batch this way...")
      } else {
        JCudfSerialization.writeToStream(columns, out, 0, numRows)
        // In this case an RDD, we want to close the batch once it is serialized out or we will
        // leak GPU memory (technically it will just wait for GC to release it and probably
        // not a lot because this is used for a broadcast that really should be small)
        // In the case of broadcast the life cycle of the object is tied to GC and there is no clean
        // way to separate the two right now.  So we accept the leak.
        columns.safeClose()
        columns = null
      }
    } finally {
      range.close()
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    val range = new NvtxRange("HostDeserializeBatch", NvtxColor.PURPLE)
    try {
      val din = new DataInputStream(in)
      header = new JCudfSerialization.SerializedTableHeader(din)
      if (!header.wasInitialized()) {
        throw new IllegalStateException("Could not read data")
      }
      buffer = HostMemoryBuffer.allocate(header.getDataLen)
      // This one is a little odd. When deserialized this object is passed to
      // SerializeConcatHostBuffersDeserializeBatch.  But we cannot close this (the input data)
      // after serializing it because in local mode it is up to spark and GC if it is the same
      // object, or if it tries to deserialize it, so that means we have to rely on GC to clean up
      // this buffer too.
      buffer.noWarnLeakExpected()
      JCudfSerialization.readTableIntoBuffer(din, header, buffer)
      if (!header.wasDataRead()) {
        throw new IllegalStateException("Could not read data")
      }
    } finally {
      range.close()
    }
  }

  def dataSize: Long = {
    JCudfSerialization.getSerializedSizeInBytes(columns, 0, numRows)
  }

  override def close(): Unit = {
    columns.safeClose()
    if (buffer != null) {
      buffer.close()
    }
  }
}
