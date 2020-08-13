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

import java.io.OutputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.math.max

import ai.rapids.cudf.{ColumnVector, DType, HostMemoryBuffer, NvtxColor, ParquetOptions, Table}
import com.nvidia.spark.RebaseHelper
import com.nvidia.spark.rapids.GpuMetricNames._
import com.nvidia.spark.rapids.ParquetPartitionReader.CopyRange
import com.nvidia.spark.rapids.RapidsConf.ENABLE_SMALL_FILES_PARQUET
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath, FileMetaData, ParquetMetadata}
import org.apache.parquet.schema.{GroupType, MessageType, Types}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetFilters, ParquetReadSupport}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, FileScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class GpuParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf)
  extends FileScan with ScanWithMetrics {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // We have to look at the latest conf because we set the input file exec used
    // conf after the rapidsConf is passed in here.
    val latestConf = new RapidsConf(sparkSession.sessionState.conf)

    logDebug(s"Small file optimization: ${rapidsConf.isParquetSmallFilesEnabled} " +
      s"Inputfile: ${latestConf.isInputFileExecUsed}")
    if (rapidsConf.isParquetSmallFilesEnabled && !latestConf.isInputFileExecUsed) {
      GpuParquetMultiPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics)
    } else {
      GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: GpuParquetScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && rapidsConf == p.rapidsConf
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}

object GpuParquetScan extends Logging {
  def tagSupport(scanMeta: ScanMeta[ParquetScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    if (scanMeta.conf.isParquetSmallFilesEnabled && scan.options.getBoolean("mergeSchema", false)) {
      scanMeta.willNotWorkOnGpu("mergeSchema is not supported yet with" +
        s" the small file optimization, disable ${ENABLE_SMALL_FILES_PARQUET.key}")
    }
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      readSchema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    val sqlConf = sparkSession.conf

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetReadEnabled) {
      meta.willNotWorkOnGpu("Parquet input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_READ} to true")
    }

    if (meta.conf.isParquetSmallFilesEnabled && sparkSession.conf
      .getOption("spark.sql.parquet.mergeSchema").exists(_.toBoolean)) {
      meta.willNotWorkOnGpu("mergeSchema is not supported yet with" +
      s" the small file optimization, disable ${ENABLE_SMALL_FILES_PARQUET.key}")
    }

    for (field <- readSchema) {
      if (!GpuColumnVector.isSupportedType(field.dataType)) {
        meta.willNotWorkOnGpu(s"GpuParquetScan does not support fields of type ${field.dataType}")
      }
    }

    val schemaHasStrings = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[StringType])
    }

    if (sqlConf.get(SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValueString).toBoolean && schemaHasStrings) {
      meta.willNotWorkOnGpu(s"GpuParquetScan does not support" +
          s" ${SQLConf.PARQUET_BINARY_AS_STRING.key}")
    }

    val schemaHasTimestamps = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }

    // Currently timestamp conversion is not supported.
    // If support needs to be added then we need to follow the logic in Spark's
    // ParquetPartitionReaderFactory and VectorizedColumnReader which essentially
    // does the following:
    //   - check if Parquet file was created by "parquet-mr"
    //   - if not then look at SQLConf.SESSION_LOCAL_TIMEZONE and assume timestamps
    //     were written in that timezone and convert them to UTC timestamps.
    // Essentially this should boil down to a vector subtract of the scalar delta
    // between the configured timezone's delta from UTC on the timestamp data.
    if (schemaHasTimestamps && sparkSession.sessionState.conf.isParquetINT96TimestampConversion) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support int96 timestamp conversion")
    }

    sqlConf.get(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key) match {
      case "EXCEPTION" => // Good
      case "CORRECTED" => // Good
      case "LEGACY" => // Good, but it really is EXCEPTION for us...
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported read rebase mode")
    }
  }
}

/**
 * Base object that has common functions for both GpuParquetPartitionReaderFactory
 * and GpuParquetPartitionReaderFactory
 */
object GpuParquetPartitionReaderFactoryBase {

  def filterClippedSchema(
      clippedSchema: MessageType,
      fileSchema: MessageType,
      isCaseSensitive: Boolean): MessageType = {
    val fs = fileSchema.asGroupType()
    val types = if (isCaseSensitive) {
      val inFile = fs.getFields.asScala.map(_.getName).toSet
      clippedSchema.asGroupType()
        .getFields.asScala.filter(f => inFile.contains(f.getName))
    } else {
      val inFile = fs.getFields.asScala
        .map(_.getName.toLowerCase(Locale.ROOT)).toSet
      clippedSchema.asGroupType()
        .getFields.asScala
        .filter(f => inFile.contains(f.getName.toLowerCase(Locale.ROOT)))
    }
    if (types.isEmpty) {
      Types.buildMessage().named("spark_schema")
    } else {
      Types
        .buildMessage()
        .addFields(types: _*)
        .named("spark_schema")
    }
  }

  // Copied from Spark
  private val SPARK_VERSION_METADATA_KEY = "org.apache.spark.version"
  // Copied from Spark
  private val SPARK_LEGACY_DATETIME = "org.apache.spark.legacyDateTime"

  def isCorrectedRebaseMode(
      lookupFileMeta: String => String,
      isCorrectedModeConfig: Boolean): Boolean = {
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by Spark 3.0 and later may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      version >= "3.0.0" && lookupFileMeta(SPARK_LEGACY_DATETIME) == null
    }.getOrElse(isCorrectedModeConfig)
  }
}

/**
 * Similar to GpuParquetPartitionReaderFactory but extended for reading multiple files
 * in an iteration. This will allow us to read multiple small files and combine them
 * on the CPU side before sending them down to the GPU.
 */
case class GpuParquetMultiPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, SQLMetric]) extends PartitionReaderFactory with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val isCorrectedRebase =
    "CORRECTED" == sqlConf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val files = filePartition.files
    buildColumnarReader(files)
  }

  def buildColumnarReader(
      partitionedFiles: Array[PartitionedFile]): PartitionReader[ColumnarBatch] = {
    buildBaseColumnarParquetReader(partitionedFiles)
  }

  private def buildBaseColumnarParquetReader(
      files: Array[PartitionedFile]): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val clippedBlocks = ArrayBuffer[MultiFilePartitionInfo]()
    var clippedSchema: MessageType = null
    var isCorrectedRebaseForThis: Option[Boolean] = None

    logWarning(s"files are: ${files.mkString(",")} for task ${TaskContext.get().partitionId()}")
    files.map { file =>
      logWarning(s"processing file: $file")
      val filePath = new Path(new URI(file.filePath))
      //noinspection ScalaDeprecation
      val footer = ParquetFileReader.readFooter(conf, filePath,
        ParquetMetadataConverter.range(file.start, file.start + file.length))
      val fileSchema = footer.getFileMetaData.getSchema
      val pushedFilters = if (enableParquetFilterPushDown) {
        val parquetFilters = new ParquetFilters(fileSchema, pushDownDate, pushDownTimestamp,
          pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
        filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
      } else {
        None
      }

      // We need to ensure all files we are going to combine have the same datetime rebase mode.
      // We could potentially handle this by just splitting the batches up but for now just error.
      val isCorrectedRebaseForThisFile =
        GpuParquetPartitionReaderFactoryBase.isCorrectedRebaseMode(
          footer.getFileMetaData.getKeyValueMetaData.get, isCorrectedRebase)
      if (!isCorrectedRebaseForThis.isDefined) {
        isCorrectedRebaseForThis = Some(isCorrectedRebaseForThisFile)
      } else if (isCorrectedRebaseForThis.get != isCorrectedRebaseForThisFile) {
        throw new UnsupportedOperationException("Can't use small file optimization when " +
          "datetime rebase mode is different across files, turn off " +
          s"${ENABLE_SMALL_FILES_PARQUET.key} and rerun.")
      }

      val blocks = if (pushedFilters.isDefined) {
        // Use the ParquetFileReader to perform dictionary-level filtering
        ParquetInputFormat.setFilterPredicate(conf, pushedFilters.get)
        //noinspection ScalaDeprecation
        val parquetReader = new ParquetFileReader(conf, footer.getFileMetaData, filePath,
          footer.getBlocks, Collections.emptyList[ColumnDescriptor])
        try {
          parquetReader.getRowGroups
        } finally {
          parquetReader.close()
        }
      } else {
        footer.getBlocks
      }

      val clippedSchemaTmp = ParquetReadSupport.clipParquetSchema(fileSchema, readDataSchema,
        isCaseSensitive)
      // ParquetReadSupport.clipParquetSchema does most of what we want, but it includes
      // everything in readDataSchema, even if it is not in fileSchema we want to remove those
      // for our own purposes
      clippedSchema = GpuParquetPartitionReaderFactoryBase.filterClippedSchema(clippedSchemaTmp,
        fileSchema, isCaseSensitive)
      val columnPaths = clippedSchema.getPaths.asScala.map(x => ColumnPath.get(x: _*))
      val clipped = ParquetPartitionReader.clipBlocks(columnPaths, blocks.asScala)
      clippedBlocks ++=
        clipped.map(MultiFilePartitionInfo(filePath, _, file.partitionValues, clippedSchema))
    }

    new MultiFileParquetPartitionReader(conf, files, clippedBlocks,
      isCaseSensitive, readDataSchema, debugDumpPrefix, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, metrics, partitionSchema,
      isCorrectedRebaseForThis.getOrElse(isCorrectedRebase))
  }
}

case class GpuParquetPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, SQLMetric]) extends FilePartitionReaderFactory with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val isCorrectedRebase =
    "CORRECTED" == sqlConf.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ)


  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val reader = buildBaseColumnarParquetReader(partitionedFile)
    ColumnarPartitionReaderWithPartitionValues.newReader(partitionedFile, reader, partitionSchema)
  }

  private def buildBaseColumnarParquetReader(
      file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val filePath = new Path(new URI(file.filePath))
    //noinspection ScalaDeprecation
    val footer = ParquetFileReader.readFooter(conf, filePath,
        ParquetMetadataConverter.range(file.start, file.start + file.length))
    val fileSchema = footer.getFileMetaData.getSchema
    val pushedFilters = if (enableParquetFilterPushDown) {
      val parquetFilters = new ParquetFilters(fileSchema, pushDownDate, pushDownTimestamp,
          pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
      filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
    } else {
      None
    }

    val isCorrectedRebaseForThis =
      GpuParquetPartitionReaderFactoryBase.isCorrectedRebaseMode(
        footer.getFileMetaData.getKeyValueMetaData.get, isCorrectedRebase)

    val blocks = if (pushedFilters.isDefined) {
      // Use the ParquetFileReader to perform dictionary-level filtering
      ParquetInputFormat.setFilterPredicate(conf, pushedFilters.get)
      //noinspection ScalaDeprecation
      val parquetReader = new ParquetFileReader(conf, footer.getFileMetaData, filePath,
        footer.getBlocks, Collections.emptyList[ColumnDescriptor])
      try {
        parquetReader.getRowGroups
      } finally {
        parquetReader.close()
      }
    } else {
      footer.getBlocks
    }

    val clippedSchemaTmp = ParquetReadSupport.clipParquetSchema(fileSchema, readDataSchema,
        isCaseSensitive)
    // ParquetReadSupport.clipParquetSchema does most of what we want, but it includes
    // everything in readDataSchema, even if it is not in fileSchema we want to remove those
    // for our own purposes
    val clippedSchema = GpuParquetPartitionReaderFactoryBase.filterClippedSchema(clippedSchemaTmp,
      fileSchema, isCaseSensitive)
    val columnPaths = clippedSchema.getPaths.asScala.map(x => ColumnPath.get(x:_*))
    val clippedBlocks = ParquetPartitionReader.clipBlocks(columnPaths, blocks.asScala)
    new ParquetPartitionReader(conf, file, filePath, clippedBlocks, clippedSchema,
        isCaseSensitive, readDataSchema, debugDumpPrefix, maxReadBatchSizeRows,
        maxReadBatchSizeBytes, metrics, isCorrectedRebaseForThis)
  }
}

/**
 * base classes with common functions for MultiFileParquetPartitionReader and ParquetPartitionReader
 */
abstract class FileParquetPartitionReaderBase(
    conf  : Configuration,
    isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    debugDumpPrefix: String,
    execMetrics: Map[String, SQLMetric]) extends PartitionReader[ColumnarBatch] with Logging
  with ScanWithMetrics with Arm {

  protected var isExhausted: Boolean = false
  protected var maxDeviceMemory: Long = 0
  protected var batch: Option[ColumnarBatch] = None
  protected val copyBufferSize = conf.getInt("parquet.read.allocation.size", 8 * 1024 * 1024)
  metrics = execMetrics

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    isExhausted = true
  }

  protected def calculateParquetFooterSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType): Long = {
    // Calculate size of the footer metadata.
    // This uses the column metadata from the original file, but that should
    // always be at least as big as the updated metadata in the output.
    val out = new CountingOutputStream(new NullOutputStream)
    writeFooter(out, currentChunkedBlocks, schema)
    out.getByteCount
  }

  protected def calculateParquetOutputSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType,
      handleMultiFiles: Boolean): (Long, Long) = {
    // start with the size of Parquet magic (at start+end) and footer length values
    var size: Long = 4 + 4 + 4

    // Calculate the total amount of column data that will be copied
    // NOTE: Avoid using block.getTotalByteSize here as that is the
    //       uncompressed size rather than the size in the file.
    size += currentChunkedBlocks.flatMap(_.getColumns.asScala.map(_.getTotalSize)).sum

    val footerSize = calculateParquetFooterSize(currentChunkedBlocks, schema)
    val extraMemory = if (handleMultiFiles) {
      // we want to add extra memory because the ColumnChunks saved in the Footer have 2 fields
      // file_offset and data_page_offset that get much larger when we are combining files.
      // Here we estimate that by taking the number of columns * number of blocks which should be
      // the number of column chunks and then saying there are 2 fields that could be larger and
      // assume max size of those would be 8 bytes worst case. So we probably allocate to much  here
      // but it shouldn't be by a huge amount and its better then having to realloc.
      val numColumnChunks = currentChunkedBlocks.head.getColumns().size() * currentChunkedBlocks.size
      logWarning(s"extra size is: ${numColumnChunks * 2 * 8}")
      numColumnChunks * 2 * 8
    } else {
      0
    }
    val totalSize = size + footerSize + extraMemory
    logWarning(s"calculated size is : $totalSize, $footerSize")
    (totalSize, footerSize)
  }

  protected def writeFooter(
      out: OutputStream,
      blocks: Seq[BlockMetaData],
      schema: MessageType): Unit = {
    val colst = blocks.map(_.getColumns.asScala.size).sum
    logWarning("number of blocks is : " + blocks.size + " columns in blockis is " + colst)

    val fileMeta = new FileMetaData(schema, Collections.emptyMap[String, String],
      ParquetPartitionReader.PARQUET_CREATOR)
    val metadataConverter = new ParquetMetadataConverter
    val footer = new ParquetMetadata(fileMeta, blocks.asJava)
    val meta = metadataConverter.toParquetMetadata(ParquetPartitionReader.PARQUET_VERSION, footer)
    logWarning("meta number of row groups is: " + meta.row_groups.size())
    // logWarning("writing footer with meta: " + meta.toString())
    // TODO - should do row groups * num columns  to get column chunks
    org.apache.parquet.format.Util.writeFileMetaData(meta, out)
  }

  protected def copyDataRange(
      range: CopyRange,
      in: FSDataInputStream,
      out: OutputStream,
      copyBuffer: Array[Byte]): Unit = {
    if (in.getPos != range.offset) {
      in.seek(range.offset)
    }
    var bytesLeft = range.length
    while (bytesLeft > 0) {
      // downcast is safe because copyBuffer.length is an int
      val readLength = Math.min(bytesLeft, copyBuffer.length).toInt
      in.readFully(copyBuffer, 0, readLength)
      out.write(copyBuffer, 0, readLength)
      bytesLeft -= readLength
    }
  }

  /**
   * Copies the data corresponding to the clipped blocks in the original file and compute the
   * block metadata for the output. The output blocks will contain the same column chunk
   * metadata but with the file offsets updated to reflect the new position of the column data
   * as written to the output.
   *
   * @param in  the input stream for the original Parquet file
   * @param out the output stream to receive the data
   * @return updated block metadata corresponding to the output
   */
  protected def copyBlocksData(
      in: FSDataInputStream,
      out: HostMemoryOutputStream,
      blocks: Seq[BlockMetaData]): Seq[BlockMetaData] = {
    var totalRows: Long = 0
    val outputBlocks = new ArrayBuffer[BlockMetaData](blocks.length)
    val copyRanges = new ArrayBuffer[CopyRange]
    var currentCopyStart = 0L
    var currentCopyEnd = 0L
    var totalBytesToCopy = 0L
    blocks.foreach { block =>
      totalRows += block.getRowCount
      val columns = block.getColumns.asScala
      val outputColumns = new ArrayBuffer[ColumnChunkMetaData](columns.length)
      columns.foreach { column =>
        // update column metadata to reflect new position in the output file
        val offsetAdjustment = out.getPos + totalBytesToCopy - column.getStartingPos
        val newDictOffset = if (column.getDictionaryPageOffset > 0) {
          column.getDictionaryPageOffset + offsetAdjustment
        } else {
          0
        }
        //noinspection ScalaDeprecation
        outputColumns += ColumnChunkMetaData.get(
          column.getPath,
          column.getPrimitiveType,
          column.getCodec,
          column.getEncodingStats,
          column.getEncodings,
          column.getStatistics,
          column.getStartingPos + offsetAdjustment,
          newDictOffset,
          column.getValueCount,
          column.getTotalSize,
          column.getTotalUncompressedSize)

        if (currentCopyEnd != column.getStartingPos) {
          if (currentCopyEnd != 0) {
            copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
          }
          currentCopyStart = column.getStartingPos
          currentCopyEnd = currentCopyStart
        }
        currentCopyEnd += column.getTotalSize
        totalBytesToCopy += column.getTotalSize
      }
      outputBlocks += ParquetPartitionReader.newParquetBlock(block.getRowCount, outputColumns)
    }

    if (currentCopyEnd != currentCopyStart) {
      copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
    }
    val copyBuffer = new Array[Byte](copyBufferSize)
    copyRanges.foreach(copyRange => copyDataRange(copyRange, in, out, copyBuffer))
    outputBlocks
  }

  protected def areNamesEquiv(groups: GroupType, index: Int, otherName: String,
      isCaseSensitive: Boolean): Boolean = {
    if (groups.getFieldCount > index) {
      if (isCaseSensitive) {
        groups.getFieldName(index) == otherName
      } else {
        groups.getFieldName(index).toLowerCase(Locale.ROOT) == otherName.toLowerCase(Locale.ROOT)
      }
    } else {
      false
    }
  }

  protected def evolveSchemaIfNeededAndClose(
      inputTable: Table,
      filePath: String,
      clippedSchema: MessageType): Table = {
    if (readDataSchema.length > inputTable.getNumberOfColumns) {
      // Spark+Parquet schema evolution is relatively simple with only adding/removing columns
      // To type casting or anyting like that
      val clippedGroups = clippedSchema.asGroupType()
      val newColumns = new Array[ColumnVector](readDataSchema.length)
      try {
        withResource(inputTable) { table =>
          var readAt = 0
          (0 until readDataSchema.length).foreach(writeAt => {
            val readField = readDataSchema(writeAt)
            if (areNamesEquiv(clippedGroups, readAt, readField.name, isSchemaCaseSensitive)) {
              newColumns(writeAt) = table.getColumn(readAt).incRefCount()
              readAt += 1
            } else {
              withResource(GpuScalar.from(null, readField.dataType)) { n =>
                newColumns(writeAt) = ColumnVector.fromScalar(n, table.getRowCount.toInt)
              }
            }
          })
          if (readAt != table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Could not find the expected columns " +
              s"$readAt out of ${table.getNumberOfColumns} from $filePath")
          }
        }
        new Table(newColumns: _*)
      } finally {
        newColumns.safeClose()
      }
    } else {
      inputTable
    }
  }

  protected def dumpParquetData(
      hmb: HostMemoryBuffer,
      dataLength: Long,
      splits: Array[PartitionedFile]): Unit = {
    val (out, path) = FileUtils.createTempFile(conf, debugDumpPrefix, ".parquet")
    try {
      logInfo(s"Writing Parquet split data for $splits to $path")
      val in = new HostMemoryInputStream(hmb, dataLength)
      IOUtils.copy(in, out)
    } finally {
      out.close()
    }
  }
}

private case class MultiFilePartitionInfo(filePath: Path, blockMeta: BlockMetaData,
    partValues: InternalRow, schema: MessageType)

/**
 * A PartitionReader that can read multiple Parquet files up to the certain size.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param split the file split to read
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
 */
class MultiFileParquetPartitionReader(
    conf: Configuration,
    splits: Array[PartitionedFile],
    clippedBlocks: Seq[MultiFilePartitionInfo],
    isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, SQLMetric],
    partitionSchema: StructType,
    isCorrectedRebaseMode: Boolean) extends
  FileParquetPartitionReaderBase(conf, isSchemaCaseSensitive,
    readDataSchema, debugDumpPrefix, execMetrics) {

  private val blockIterator: BufferedIterator[MultiFilePartitionInfo] =
    clippedBlocks.iterator.buffered

  private def addPartitionValues(
      batch: Option[ColumnarBatch],
      inPartitionValues: InternalRow): Option[ColumnarBatch] = {
    batch.map { cb =>
      val partitionValues = inPartitionValues.toSeq(partitionSchema)
      val partitionScalarTypes = partitionSchema.fields.map(_.dataType)
      val partitionScalars = partitionValues.zip(partitionScalarTypes).map {
        case (v, t) => GpuScalar.from(v, t)
      }.toArray
      try {
        ColumnarPartitionReaderWithPartitionValues.addPartitionValues(cb, partitionScalars)
      } finally {
        partitionScalars.foreach(_.close())
      }
    }
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (!isExhausted) {
      if (!blockIterator.hasNext) {
        isExhausted = true
        metrics("peakDevMemory") += maxDeviceMemory
      } else {
        batch = readBatch()
      }
    }
    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  private def readPartFiles(
      blocks: Seq[(Path, BlockMetaData)],
      currentClippedSchema: MessageType): (HostMemoryBuffer, Long) = {
    val nvtxRange = new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))
    // ugly but we want to keep the order
    val filesAndBlocks = LinkedHashMap[Path, ArrayBuffer[BlockMetaData]]()
    blocks.foreach { info =>
      if (filesAndBlocks.contains(info._1)) {
        filesAndBlocks(info._1) += info._2
      } else {
        filesAndBlocks(info._1) = ArrayBuffer(info._2)
      }
    }

    try {
      var succeeded = false
      val allBlocks = blocks.map(_._2)
      val (estTotalSize, estFooterSize) =
        calculateParquetOutputSize(allBlocks, currentClippedSchema, false)
      logWarning(s"calculated size for hostmemory buffer: $estTotalSize")
      var hmb = HostMemoryBuffer.allocate(estTotalSize)
      var out = new HostMemoryOutputStream(hmb)
      try {
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        val allOutputBlocks = scala.collection.mutable.ArrayBuffer[BlockMetaData]()
        filesAndBlocks.foreach { case (file, blocks) =>
          val in = file.getFileSystem(conf).open(file)
          try {
            val retBlocks = copyBlocksData(in, out, blocks)
            allOutputBlocks ++= retBlocks
          } finally {
            in.close()
          }
        }
        // Not sure how expensive this is, we could throw exception instead if the written
        // size comes out > then the estimated size
        val actualFooterSize = calculateParquetFooterSize(allOutputBlocks, currentClippedSchema)
        val footerPos = out.getPos
        logWarning(s"actual write before write footer count is: ${footerPos} actual " +
          s"footer size: $actualFooterSize")
        // The footer size can change vs the estimated because we are combining more blocks and
        // offsets are larger, check to make sure we allocated enough memory before writing
        // the footer. 4 + 4 is for writing size and the ending PARQUET_MAGIC.
        val newSizeEstimate = footerPos + actualFooterSize + 4 + 4
        if (newSizeEstimate > estTotalSize) {
          // realloc memory and copy
          logWarning(s"the original estimated size $estTotalSize is to small, " +
            s"reallocing and copying data to bigger buffer size: $newSizeEstimate")
          val newhmb = HostMemoryBuffer.allocate(newSizeEstimate)
          var copySucceeded = false
          try {
            val newout = new HostMemoryOutputStream(newhmb)
            val in = new HostMemoryInputStream(hmb, footerPos)
            IOUtils.copy(in, out)
            out = newout
            copySucceeded = true
          } finally {
            if (!copySucceeded) {
              newhmb.close()
            }
          }
          val prevhmb = hmb
          hmb = newhmb
          prevhmb.close()
        } else {
          out
        }
        writeFooter(out, allOutputBlocks, currentClippedSchema)
        logWarning(s"actual write after write footer count is: ${out.getPos}")
        BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        succeeded = true
        logWarning(s"Actual written out size: ${out.getPos}")
        // triple check we didn't go over memory
        if (out.getPos > estTotalSize) {
          throw new QueryExecutionException(s"Calculated buffer size $estTotalSize is to " +
            s"small, actual: ${out.getPos}")
        }
        (hmb, out.getPos)
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
    } finally {
      nvtxRange.close()
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val nvtxRange = new NvtxWithMetrics("Parquet readBatch", NvtxColor.GREEN, metrics(TOTAL_TIME))
    try {
      val (currentClippedSchema, partitionValues, currentChunkedBlocks) =
        populateCurrentBlockChunk()
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_._2.getRowCount).sum.toInt
        if (numRows == 0) {
          None
        } else {
          Some(new ColumnarBatch(Array.empty, numRows.toInt))
        }
      } else {
        val table = readToTable(currentChunkedBlocks, currentClippedSchema)
        try {
          val maybeBatch = table.map(GpuColumnVector.from)
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          val withPartitionValues = addPartitionValues(maybeBatch, partitionValues)
          withPartitionValues
        } finally {
          table.foreach(_.close())
        }
      }
    } finally {
      nvtxRange.close()
    }
  }

  private def readToTable(
      currentChunkedBlocks: Seq[(Path, BlockMetaData)],
      currentClippedSchema: MessageType): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }

    val (dataBuffer, dataSize) = readPartFiles(currentChunkedBlocks, currentClippedSchema)
    try {
      if (dataSize == 0) {
        None
      } else {
        if (debugDumpPrefix != null) {
          dumpParquetData(dataBuffer, dataSize, splits)
        }
        val fieldNames = currentClippedSchema.asGroupType()
          .getFields.asScala.map(_.getName).toArray
        logWarning("getting fieldsNames: " + fieldNames.mkString(","))
        val parseOpts = ParquetOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .includeColumn(fieldNames:_*).build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        logWarning("readDataSchema is : " + readDataSchema.fieldNames.mkString(","))
        val table = Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
        if (!isCorrectedRebaseMode) {
          (0 until table.getNumberOfColumns).foreach { i =>
            if (RebaseHelper.isDateTimeRebaseNeededRead(table.getColumn(i))) {
              throw RebaseHelper.newRebaseExceptionInRead("Parquet")
            }
          }
        }
        logWarning("actual columsn " + table.getNumberOfColumns)
        maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
        if (readDataSchema.length < table.getNumberOfColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${table.getNumberOfColumns} from $currentChunkedBlocks")
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(evolveSchemaIfNeededAndClose(table, splits.mkString(","), currentClippedSchema))
      }
    } finally {
      dataBuffer.close()
    }
  }

  private def populateCurrentBlockChunk():
    (MessageType, InternalRow, Seq[(Path, BlockMetaData)]) = {

    val currentChunk = new ArrayBuffer[(Path, BlockMetaData)]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numParquetBytes: Long = 0
    var currentFile: Path = null
    var currentPartitionValues: InternalRow = null
    var currentClippedSchema: MessageType = null

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        if (currentFile == null) {
          currentFile = blockIterator.head.filePath
          currentPartitionValues = blockIterator.head.partValues
          currentClippedSchema = blockIterator.head.schema
        }
        if (currentFile != blockIterator.head.filePath) {
          // check to see if partitionValues different, then have to split it
          if (blockIterator.head.partValues != currentPartitionValues) {
            logWarning(s"Partition values for the next file ${blockIterator.head.filePath}" +
              s" doesn't match current $currentFile, splitting it into another batch!")
            return
          }
          val schemaNewfile =
            blockIterator.head.schema.asGroupType().getFields.asScala.map(_.getName)
          val schemaCurrentfile =
            currentClippedSchema.asGroupType().getFields.asScala.map(_.getName)
          if (schemaNewfile.sameElements(schemaCurrentfile)) {
            logWarning(s"File schema for the next file ${blockIterator.head.filePath}" +
              s" doesn't match current $currentFile, splitting it into another batch!")
            return
          }
          currentFile = blockIterator.head.filePath
          currentPartitionValues = blockIterator.head.partValues
          currentClippedSchema = blockIterator.head.schema
        }
        val peekedRowGroup = blockIterator.head.blockMeta
        if (peekedRowGroup.getRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }

        if (numRows == 0 || numRows + peekedRowGroup.getRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedRowGroup.getRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            val nextBlock = blockIterator.next()
            val nextTuple = (nextBlock.filePath, nextBlock.blockMeta)
            currentChunk += nextTuple
            numRows += currentChunk.last._2.getRowCount
            numParquetBytes += currentChunk.last._2.getTotalByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()

    logDebug(s"Loaded $numRows rows from Parquet. Parquet bytes read: $numParquetBytes. " +
      s"Estimated GPU bytes: $numBytes")

    (currentClippedSchema, currentPartitionValues, currentChunk)
  }
}

/**
 * A PartitionReader that reads a Parquet file split on the GPU.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param split the file split to read
 * @param filePath the path to the Parquet file
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param clippedParquetSchema the Parquet schema from the original Parquet file that has been
 *                             clipped to contain only the columns to be read
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
 */
class ParquetPartitionReader(
    conf: Configuration,
    split: PartitionedFile,
    filePath: Path,
    clippedBlocks: Seq[BlockMetaData],
    clippedParquetSchema: MessageType,
    isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, SQLMetric],
    isCorrectedRebaseMode: Boolean)  extends
  FileParquetPartitionReaderBase(conf,
    isSchemaCaseSensitive, readDataSchema, debugDumpPrefix, execMetrics) {

  private val blockIterator :  BufferedIterator[BlockMetaData] = clippedBlocks.iterator.buffered

  override def next(): Boolean = {
    // logWarning(s"calling partition reader $filePath")
    batch.foreach(_.close())
    batch = None
    if (!isExhausted) {
      if (!blockIterator.hasNext) {
        isExhausted = true
        metrics("peakDevMemory") += maxDeviceMemory
      } else {
        batch = readBatch()
      }
    }
    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  private def readPartFile(blocks: Seq[BlockMetaData]): (HostMemoryBuffer, Long) = {
    val nvtxRange = new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))
    try {
      val in = filePath.getFileSystem(conf).open(filePath)
      // logWarning(s"origin processing file: $filePath")
      try {
        var succeeded = false
        val (totalSize, footerSize) =
          calculateParquetOutputSize(blocks, clippedParquetSchema, false)
        val hmb =
          HostMemoryBuffer.allocate(totalSize)
        try {
          val out = new HostMemoryOutputStream(hmb)
          out.write(ParquetPartitionReader.PARQUET_MAGIC)
          val outputBlocks = copyBlocksData(in, out, blocks)
          val footerPos = out.getPos
          writeFooter(out, outputBlocks, clippedParquetSchema)
          BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
          out.write(ParquetPartitionReader.PARQUET_MAGIC)
          succeeded = true
          (hmb, out.getPos)
        } finally {
          if (!succeeded) {
            hmb.close()
          }
        }
      } finally {
        in.close()
      }
    } finally {
      nvtxRange.close()
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val nvtxRange = new NvtxWithMetrics("Parquet readBatch", NvtxColor.GREEN, metrics(TOTAL_TIME))
    try {
      val currentChunkedBlocks = populateCurrentBlockChunk()
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_.getRowCount).sum.toInt
        if (numRows == 0) {
          None
        } else {
          Some(new ColumnarBatch(Array.empty, numRows.toInt))
        }
      } else {
        val table = readToTable(currentChunkedBlocks)
        try {
          val maybeBatch = table.map(GpuColumnVector.from)
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          maybeBatch
        } finally {
          table.foreach(_.close())
        }
      }
    } finally {
      nvtxRange.close()
    }
  }


  private def readToTable(currentChunkedBlocks: Seq[BlockMetaData]): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }
    // logWarning("orig read to table blocks is " + currentChunkedBlocks.mkString(","))
    val (dataBuffer, dataSize) = readPartFile(currentChunkedBlocks)
    try {
      if (dataSize == 0) {
        None
      } else {
        if (debugDumpPrefix != null) {
          dumpParquetData(dataBuffer, dataSize, Array(split))
        }
        val fieldNames = clippedParquetSchema.asGroupType()
          .getFields.asScala.map(_.getName).toArray
        logWarning("getting fieldsNames: " + fieldNames.mkString(","))
        val parseOpts = ParquetOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .includeColumn(fieldNames:_*).build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val table = Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
        if (!isCorrectedRebaseMode) {
          (0 until table.getNumberOfColumns).foreach { i =>
            if (RebaseHelper.isDateTimeRebaseNeededRead(table.getColumn(i))) {
              throw RebaseHelper.newRebaseExceptionInRead("Parquet")
            }
          }
        }
        maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
        logWarning("readDataSchema is : " + readDataSchema.fieldNames.mkString(","))
        logWarning("actual columsn " + table.getNumberOfColumns)
        if (readDataSchema.length < table.getNumberOfColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${table.getNumberOfColumns} from $filePath")
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(evolveSchemaIfNeededAndClose(table, filePath.toString, clippedParquetSchema))
      }
    } finally {
      dataBuffer.close()
    }
  }

  private def populateCurrentBlockChunk(): Seq[BlockMetaData] = {

    val currentChunk = new ArrayBuffer[BlockMetaData]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numParquetBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        val peekedRowGroup = blockIterator.head
        if (peekedRowGroup.getRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.getRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedRowGroup.getRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIterator.next()
            numRows += currentChunk.last.getRowCount
            numParquetBytes += currentChunk.last.getTotalByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()

    logDebug(s"Loaded $numRows rows from Parquet. Parquet bytes read: $numParquetBytes. " +
       s"Estimated GPU bytes: $numBytes")

    currentChunk
  }
}

object ParquetPartitionReader {
  private[rapids] val PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private[rapids] val PARQUET_CREATOR = "RAPIDS Spark Plugin"
  private[rapids] val PARQUET_VERSION = 1

  private[rapids] case class CopyRange(offset: Long, length: Long)

  /**
   * Build a new BlockMetaData
   *
   * @param rowCount the number of rows in this block
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  private[rapids] def newParquetBlock(
      rowCount: Long,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(rowCount)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }

  /**
   * Trim block metadata to contain only the column chunks that occur in the specified columns.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param columnPaths the paths of columns to preserve
   * @param blocks the block metadata from the original Parquet file
   * @return the updated block metadata with undesired column chunks removed
   */
  private[spark] def clipBlocks(columnPaths: Seq[ColumnPath],
      blocks: Seq[BlockMetaData]): Seq[BlockMetaData] = {
    val pathSet = columnPaths.toSet
    blocks.map(oldBlock => {
      //noinspection ScalaDeprecation
      val newColumns = oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath))
      ParquetPartitionReader.newParquetBlock(oldBlock.getRowCount, newColumns)
    })
  }
}


