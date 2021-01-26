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

import ai.rapids.cudf._
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.sql.vectorized.rapids.AccessibleArrowColumnVector

object HostColumnarToGpu extends Logging {

  def arrowColumnarCopy(
      cv: ColumnVector,
      ab: ai.rapids.cudf.ArrowColumnBuilder,
      nullable: Boolean,
      rows: Int): Unit = {
    logWarning("Arrow host columnar to gpu cv is type: " + cv.getClass().toString())
    if (cv.isInstanceOf[AccessibleArrowColumnVector]) {
      logWarning("looking at arrow column vector")
      // TODO - how make sure off heap?
      // could create HostMemoryBuffer(addr, length)'
      val arrowVec = cv.asInstanceOf[AccessibleArrowColumnVector]

      // TODO - accessor is private to ArrowColumnVector!!!
      // ValueVector => ArrowBuf

      val buffers = arrowVec.getArrowValueVector.getBuffers(false)
      logWarning("num buffers is " + buffers.size)

      val arrowDataAddr = arrowVec.getArrowValueVector.getDataBuffer.memoryAddress()
      val arrowDataLen = arrowVec.getArrowValueVector.getBufferSize() // ?
      val arrowDataMem = arrowVec.getArrowValueVector.getDataBuffer.getActualMemoryConsumed() // ?

      val arrowDataCap = arrowVec.getArrowValueVector.getDataBuffer.capacity() // ? 80
      val arrowDataVals = arrowVec.getArrowValueVector.getValueCount() // 20
      logWarning(s"arrow data lenght is: $arrowDataLen capcity $arrowDataCap memory: $arrowDataMem num values $arrowDataVals")
      // val hostDataBuf = new HostMemoryBuffer(arrowDataAddr, arrowDataMem, null)
      ab.setDataBuf(arrowDataAddr, arrowDataMem)
      // TODO - need to check null count as validiting isn't required
      val nullCount = arrowVec.getArrowValueVector.getNullCount()
      ab.setNullCount(nullCount)
      logWarning("null count is " + nullCount)
     //  if (nullCount > 0) {
        val validity = arrowVec.getArrowValueVector.getValidityBuffer.memoryAddress()
        val validityLen = arrowVec.getArrowValueVector.getValidityBuffer.getActualMemoryConsumed()
        ab.setValidityBuf(validity, validityLen)
     //  }

      try {
        // TODO - should we chekc types first instead?
        val arrowDataOffsetBuf = arrowVec.getArrowValueVector.getOffsetBuffer
        if (arrowDataOffsetBuf != null) {
          logWarning("arrow data offset buffer addrs: " + arrowDataOffsetBuf.memoryAddress())
          val arrowDataOffsetLen = arrowVec.getArrowValueVector.getOffsetBuffer.getActualMemoryConsumed()
          // val hostValidBuf = new HostMemoryBuffer(arrowDataOffsetBuf.memoryAddress(), arrowDataOffsetLen)
          ab.setOffsetBuf(arrowDataOffsetBuf.memoryAddress(), arrowDataOffsetLen)
        } else {
          logWarning("arrow data offset buffer is null")
        }
      } catch {
        case e: UnsupportedOperationException =>
          logWarning("unsupported op getOffsetBuffer")
      }


    } else {
      throw new Exception("not arrow data shouldn't be here!")
    }
  }

  def columnarCopy(cv: ColumnVector, b: ai.rapids.cudf.HostColumnVector.ColumnBuilder,
      nullable: Boolean, rows: Int): Unit = {
    logWarning("Not Arrow host columnar to gpu cv is type: " + cv.getClass().toString())
    (cv.dataType(), nullable) match {
      case (ByteType | BooleanType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getByte(i))
          }
        }
      case (ByteType | BooleanType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getByte(i))
        }
      case (ShortType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getShort(i))
          }
        }
      case (ShortType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getShort(i))
        }
      case (IntegerType | DateType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getInt(i))
          }
        }
      case (IntegerType | DateType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getInt(i))
        }
      case (LongType | TimestampType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getLong(i))
          }
        }
      case (LongType | TimestampType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getLong(i))
        }
      case (FloatType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getFloat(i))
          }
        }
      case (FloatType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getFloat(i))
        }
      case (DoubleType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.append(cv.getDouble(i))
          }
        }
      case (DoubleType, false) =>
        for (i <- 0 until rows) {
          b.append(cv.getDouble(i))
        }
      case (StringType, true) =>
        for (i <- 0 until rows) {
          if (cv.isNullAt(i)) {
            b.appendNull()
          } else {
            b.appendUTF8String(cv.getUTF8String(i).getBytes)
          }
        }
      case (StringType, false) =>
        for (i <- 0 until rows) {
          b.appendUTF8String(cv.getUTF8String(i).getBytes)
        }
      case (NullType, true) =>
        for (_ <- 0 until rows) {
          b.appendNull()
        }
      case (dt: DecimalType, nullable) =>
        // Because DECIMAL64 is the only supported decimal DType, we can
        // append unscaledLongValue instead of BigDecimal itself to speedup this conversion.
        // If we know that the value is WritableColumnVector we could
        // speed this up even more by getting the unscaled long or int directly.
        if (nullable) {
          for (i <- 0 until rows) {
            if (cv.isNullAt(i)) {
              b.appendNull()
            } else {
              // The precision here matters for cpu column vectors (such as OnHeapColumnVector).
              b.append(cv.getDecimal(i, dt.precision, dt.scale).toUnscaledLong)
            }
          }
        } else {
          for (i <- 0 until rows) {
            b.append(cv.getDecimal(i, dt.precision, dt.scale).toUnscaledLong)
          }
        }
      case (t, _) =>
        throw new UnsupportedOperationException(s"Converting to GPU for $t is not currently " +
          s"supported")
    }
  }
}

/**
 * This iterator builds GPU batches from host batches. The host batches potentially use Spark's
 * UnsafeRow so it is not safe to cache these batches. Rows must be read and immediately written
 * to CuDF builders.
 */
class HostToGpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    goal: CoalesceGoal,
    schema: StructType,
    numInputRows: SQLMetric,
    numInputBatches: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    collectTime: SQLMetric,
    concatTime: SQLMetric,
    totalTime: SQLMetric,
    peakDevMemory: SQLMetric,
    opName: String)
  extends AbstractGpuCoalesceIterator(iter,
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    collectTime,
    concatTime,
    totalTime,
    opName) {

  // RequireSingleBatch goal is intentionally not supported in this iterator
  assert(goal != RequireSingleBatch)

  var batchBuilder: GpuColumnVector.GpuColumnarBatchBuilderBase = _
  var totalRows = 0
  var maxDeviceMemory: Long = 0

  /**
   * Initialize the builders using an estimated row count based on the schema and the desired
   * batch size defined by [[RapidsConf.GPU_BATCH_SIZE_BYTES]].
   */
  override def initNewBatch(batch: ColumnarBatch): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }

    logWarning(" in init new batch cols is:" + batch.numCols())
      // when reading host batches it is essential to read the data immediately and pass to a
      // builder and we need to determine how many rows to allocate in the builder based on the
      // schema and desired batch size

      // TODO - batch row limit right for arrow?  Do we want to allow splitting or just single?
      batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes,
        GpuBatchUtils.estimateGpuMemory(schema, 512), 512)

      // if no columns then probably a count operation so doesn't matter which builder we use
      val isArrow = if (batch.numCols() > 0 && batch.column(0).isInstanceOf[AccessibleArrowColumnVector]) {
        true;
      }  else {
        false
      }
      if (batch.numRows() > batchRowLimit) {
        logWarning(s"batch num rows: ${batch.numRows()} > then row limit: $batchRowLimit")
      }
      if (isArrow) {
        logWarning("arrow batch builder")
        // todo - REMOVE BATCH ROWLIMIT
        batchBuilder =
          new GpuColumnVector.GpuArrowColumnarBatchBuilder(schema, batchRowLimit, batch)
      } else {
        logWarning("not an arrow batch builder")
        batchBuilder = new GpuColumnVector.GpuColumnarBatchBuilder(schema, batchRowLimit, null)
     }
    totalRows = 0
  }

  override def addBatchToConcat(batch: ColumnarBatch): Unit = {
    logWarning("in add batch")
    val rows = batch.numRows()
    for (i <- 0 until batch.numCols()) {
      batchBuilder.copyColumnar(batch.column(i), i, schema.fields(i).nullable, rows)
    }
    logWarning("done in add batch")
    totalRows += rows
  }

  override def getBatchDataSize(batch: ColumnarBatch): Long = {
    schema.fields.indices.map(GpuBatchUtils.estimateGpuMemory(schema, _, batch.numRows())).sum
  }

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    logWarning("concatallandput on gpu")
    // About to place data back on the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    logWarning("batch builder build total Rows: " + totalRows)
    if (batchBuilder == null) {
      logWarning("batch builder is null");
    }
    val ret = batchBuilder.build(totalRows)
    maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(ret)

    // refine the estimate for number of rows based on this batch
    batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes, maxDeviceMemory,
      ret.numRows())

    ret
  }

  override def cleanupConcatIsDone(): Unit = {
    if (batchBuilder != null) {
      logWarning("cleanup concat done")
      batchBuilder.close()
      batchBuilder = null
    }
    totalRows = 0
    peakDevMemory.set(maxDeviceMemory)
  }

  private var onDeck: Option[ColumnarBatch] = None

  override protected def hasOnDeck: Boolean = onDeck.isDefined
  override protected def saveOnDeck(batch: ColumnarBatch): Unit = onDeck = Some(batch)
  override protected def clearOnDeck(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }
  override protected def popOnDeck(): ColumnarBatch = {
    val ret = onDeck.get
    onDeck = None
    ret
  }
}

/**
 * Put columnar formatted data on the GPU.
 */
case class HostColumnarToGpu(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode
  with GpuExec {
  import GpuMetricNames._

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    NUM_INPUT_ROWS ->
      SQLMetrics.createMetric(sparkContext, GpuMetricNames.DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES ->
      SQLMetrics.createMetric(sparkContext, GpuMetricNames.DESCRIPTION_NUM_INPUT_BATCHES),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time"),
    "peakDevMemory" ->
      SQLMetrics.createMetric(sparkContext, GpuMetricNames.DESCRIPTION_PEAK_DEVICE_MEMORY)
  )

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def outputBatching: CoalesceGoal = goal

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  /**
   * Returns an RDD[ColumnarBatch] that when mapped over will produce GPU-side column vectors
   * that are expected to be closed by its caller, not [[HostColumnarToGpu]].
   *
   * The expectation is that the only valid instantiation of this node is
   * as a child of a GPU exec node.
   *
   * @return an RDD of `ColumnarBatch`
   */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val numInputRows = longMetric(NUM_INPUT_ROWS)
    val numInputBatches = longMetric(NUM_INPUT_BATCHES)
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val totalTime = longMetric(TOTAL_TIME)
    val peakDevMemory = longMetric("peakDevMemory")

    // cache in a local to avoid serializing the plan
    val outputSchema = schema

    val batches = child.executeColumnar()

    batches.mapPartitions { iter =>
      new HostToGpuCoalesceIterator(iter, goal, outputSchema,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches, collectTime, concatTime,
        totalTime, peakDevMemory, "HostColumnarToGpu")
    }
  }
}
