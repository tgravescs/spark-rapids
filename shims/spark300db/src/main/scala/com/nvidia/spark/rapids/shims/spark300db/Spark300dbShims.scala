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

package com.nvidia.spark.rapids.shims.spark300db

import java.time.ZoneId

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark300.Spark300Shims
import org.apache.spark.sql.rapids.shims.spark300db._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{PartitionedFileUtil, _}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, DbPartitioningUtils, FilePartition, HadoopFsRelation, InMemoryFileIndex, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, HashJoin, SortMergeJoinExec}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, GpuTimeSub}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExecBase, GpuBroadcastMeta, GpuBroadcastNestedLoopJoinExecBase, GpuShuffleExchangeExecBase, GpuShuffleMeta}
import org.apache.spark.sql.types._
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.internal.Logging
import com.databricks.sql.transaction.tahoe.stats.PreparedDeltaFileIndex
import com.databricks.sql.transaction.tahoe.stats.DeltaScan

class Spark300dbShims extends Spark300Shims with Logging {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getGpuBroadcastNestedLoopJoinShim(
      left: SparkPlan,
      right: SparkPlan,
      join: BroadcastNestedLoopJoinExec,
      joinType: JoinType,
      condition: Option[Expression],
      targetSizeBytes: Long): GpuBroadcastNestedLoopJoinExecBase = {
    GpuBroadcastNestedLoopJoinExec(left, right, join, joinType, condition, targetSizeBytes)
  }

  override def getGpuBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): GpuBroadcastExchangeExecBase = {
    GpuBroadcastExchangeExec(mode, child)
  }

  override def isGpuHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuHashJoin => true
      case p => false
    }
  }

  override def isGpuBroadcastHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuBroadcastHashJoinExec => true
      case p => false
    }
  }

  override def isGpuShuffledHashJoin(plan: SparkPlan): Boolean = {
    plan match {
      case _: GpuShuffledHashJoinExec => true
      case p => false
    }
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    Seq(
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options

            val location = if (conf.alluxioEnabled
              && wrapped.relation.location.getClass.getCanonicalName() ==
              "com.databricks.sql.transaction.tahoe.stats.PreparedDeltaFileIndex") {

              val preparedDeltaFileIndex = wrapped.relation.location.asInstanceOf[PreparedDeltaFileIndex]
              val deltaScanFileLength = preparedDeltaFileIndex.preparedScan.files.length
              logInfo("Gary-Alluxio - deltaScanFileLength : " + deltaScanFileLength)
              logInfo("Gary-Alluxio deltascan partitionFilters:" + preparedDeltaFileIndex.preparedScan.partitionFilters)
              logInfo("Gary-Alluxio deltascan dataFilters:" + preparedDeltaFileIndex.preparedScan.dataFilters)
              logInfo("Gary-Alluxio deltascan unusedFilters:" + preparedDeltaFileIndex.preparedScan.unusedFilters)
              logInfo("Gary-Alluxio deltascan allFilters:" + preparedDeltaFileIndex.preparedScan.allFilters)
              logInfo("Gary-Alluxio -deltascan-- location:" + wrapped.relation.location)
//              logInfo("Gary-Alluxio get preparedScan " + wrapped.relation.location.preparedScan)

              // Need to change the IP address of Alluxio
//              val paths = wrapped.relation.location.inputFiles.map(str =>
//                new Path(str.replaceFirst("s3:/", "alluxio://" + conf.alluxioIPPort))).toSeq

//              logInfo("Gary-Alluxio-paths: " + paths.mkString(","))

//              val partitionDirectory = wrapped.relation.location.listFiles(Nil, Nil)
//              var sum = 0
//              partitionDirectory.foreach(x => sum = sum + x.files.length)

//              logInfo("Gary-Alluxio original partitionDirectory total files:" + sum)

//              for (dir <- partitionDirectory) {
//                logInfo("Gary-Alluxio partitionDir: " + dir.toString())
//              }

              logInfo("Gary-Alluxio-original rootpath:" + wrapped.relation.location.rootPaths.mkString(","))

              // test code to generate PartitionSpec
              // need to do as below
              // 1. partitionPaths = paths.map
//              val partitionPaths = paths.map(path => path.getParent())
//              logInfo("Gary-Alluxio partitionPaths: " + partitionPaths.mkString(","))
//
//              options.foreach(x => logInfo("Gary-Alluxio options: " + x._1 + " = " + x._2))

              val alluxioStr = "alluxio://" + conf.alluxioIPPort

              // we need rootPaths from PreparedDeltaFileIndex to infer PartitionSpec
              val finalRootPaths = wrapped.relation.location.rootPaths.map(path => {
                new Path(path.toString.replaceFirst("s3:/", alluxioStr))
              })
              logInfo("Gary-Alluxio replaced rootPaths:" + finalRootPaths.mkString(","))

              // listFiles prefixed by s3://
              val listFiles: Seq[PartitionDirectory] = wrapped.relation.location.listFiles(
                wrapped.partitionFilters, wrapped.dataFilters)
              var sum = 0
              listFiles.foreach(x => sum = sum + x.files.length)
              logInfo("Gary-Alluxio original listFiles sum :" + sum)

              // all files replaced s3:/ to alluxio://
              val inputFiles: Seq[Path] = listFiles.flatMap(partitionDir => {
                partitionDir.files.map(f => new Path(
                  f.getPath.toString.replaceFirst("s3:/", "alluxio://" + conf.alluxioIPPort)))
              }).toSet.toSeq

              logInfo("Gary-Alluxio input file size:" + inputFiles.length)
//              inputFiles.foreach(file => logInfo("Gary-Alluxio: input file " + file.toString))

              // get the leaf dir of inputFiles
              val leafDirs = inputFiles.map(_.getParent).toSet.toSeq
              logInfo("Gary-Alluxio leafDirs size:" + leafDirs.length)

              val partitionSpec = DbPartitioningUtils.inferPartitioning(
                sparkSession,
                leafDirs,
                finalRootPaths.toSet,
                wrapped.relation.options,
                Option(wrapped.relation.dataSchema)
              )

              val fileIndex = new InMemoryFileIndex(
                sparkSession,
                inputFiles,
                // for temp solution
//                options + (PartitioningAwareFileIndex.BASE_PATH_PARAM -> realRootPaths(0).toString),
                options,
                Option(wrapped.relation.dataSchema),
                userSpecifiedPartitionSpec = Some(partitionSpec)
              )

//              val partitionDirectory1 = fileIndex.listFiles(Nil, Nil)
//              sum = 0
//              partitionDirectory1.foreach(x => sum = sum + x.files.length)
//              logInfo("Gary-Alluxio final partitionDirectory total files:" + sum)

              logInfo("Gary-Alluxio partitionSpec: " + fileIndex.partitionSpec().partitionColumns)
              fileIndex
            } else {
              logInfo("Gary-Alluxio-paths: no change")
              wrapped.relation.location
            }

            logInfo("Gary-Alluxio: partitionSchema0: " + wrapped.relation.location.partitionSchema.treeString)
//            logInfo("Gary-Alluxio: " + location.inputFiles.mkString(","))
            logInfo("Gary-Alluxio: location class name:" + location.getClass.getCanonicalName)
            logInfo("Gary-Alluxio: partitionSchema1: " + wrapped.relation.partitionSchema.treeString)
            logInfo("Gary-Alluxio: dataSchema: " + wrapped.relation.dataSchema.treeString)
            logInfo("Gary-Alluxio: bucketSpec: " + wrapped.relation.bucketSpec.toString())
            logInfo("Gary-Alluxio: fileFormat: " + wrapped.relation.fileFormat.toString())
            logInfo("Gary-Alluxio: options: " + options.toString())
            logInfo("Gary-Alluxio: partitionFilters:" + wrapped.partitionFilters)
            logInfo("Gary-Alluxio: dataFilters:" + wrapped.dataFilters)
            logInfo("Gary-Alluxio: tableIdentifier:" + wrapped.tableIdentifier)



            val newRelation = HadoopFsRelation(
              location,
              wrapped.relation.partitionSchema,
              wrapped.relation.dataSchema,
              wrapped.relation.bucketSpec,
              GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
              options)(sparkSession)
            val canUseSmallFileOpt = newRelation.fileFormat match {
              case _: ParquetFileFormat => conf.isParquetMultiThreadReadEnabled
              case _ => false
            }
            GpuFileSourceScanExec(
              newRelation,
              wrapped.output,
              wrapped.requiredSchema,
              wrapped.partitionFilters,
              wrapped.optionalBucketSet,
              // TODO: Does Databricks have coalesced bucketing implemented?
              None,
              wrapped.dataFilters,
              wrapped.tableIdentifier,
              canUseSmallFileOpt)
          }
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  }

  override def getBuildSide(join: HashJoin): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  override def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
    GpuJoinUtils.getGpuBuildSide(join.buildSide)
  }

  // Databricks has a different version of FileStatus
  override def getPartitionFileNames(
      partitions: Seq[PartitionDirectory]): Seq[String] = {
    val files = partitions.flatMap(partition => partition.files)
    files.map(_.getPath.getName)
  }

  override def getPartitionFileStatusSize(partitions: Seq[PartitionDirectory]): Long = {
    partitions.map(_.files.map(_.getLen).sum).sum
  }

  override def getPartitionedFiles(
      partitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    partitions.flatMap { p =>
      p.files.map { f =>
        val partitionedFile = PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
//        val builder = new StringBuilder("Gary-Alluxio locality info: " + partitionedFile.filePath)
//        partitionedFile.locations.foreach(loc => builder.append(loc + " "))
//        logInfo(builder.toString())
        partitionedFile
      }
    }
  }

  override def getPartitionSplitFiles(
      partitions: Array[PartitionDirectory],
      maxSplitBytes: Long,
      relation: HadoopFsRelation): Array[PartitionedFile] = {
    partitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        val partitionedFiles = PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )

        val partitionedFile = partitionedFiles(0)
//        val builder = new StringBuilder("Gary-Alluxio locality info: " + partitionedFile.filePath)
//        partitionedFile.locations.foreach(loc => builder.append(loc + " "))
//        logInfo(builder.toString())

        partitionedFiles
      }
    }
  }

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition]): RDD[InternalRow] = {
    new GpuFileScanRDD(sparkSession, readFunction, filePartitions)
  }

  override def createFilePartition(index: Int, files: Array[PartitionedFile]): FilePartition = {
    FilePartition(index, files)
  }

  override def copyFileSourceScanExec(scanExec: GpuFileSourceScanExec,
      supportsSmallFileOpt: Boolean): GpuFileSourceScanExec = {
    scanExec.copy(supportsSmallFileOpt=supportsSmallFileOpt)
  }
}
