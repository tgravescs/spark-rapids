/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import java.math.{MathContext, RoundingMode}
import java.util.{Arrays, Locale}

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.duration._

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

case class StageMetrics(numTasks: Int, duration: String)

/**
 * CollectInformation mainly gets information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: Seq[ApplicationInfo]) extends Logging {

  var sqlAccums: Option[Map[Int, Seq[SQLAccumProfileResults]]] = None

  def getAppInfo: Seq[AppInfoProfileResults] = {
    val allRows = apps.map { app =>
      val a = app.appInfo
      AppInfoProfileResults(app.index, a.appName, a.appId,
        a.sparkUser,  a.startTime, a.endTime, a.duration,
        a.durationStr, a.sparkVersion, a.pluginEnabled)
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex))
    } else {
      Seq.empty
    }
  }

  // get rapids-4-spark and cuDF jar if CPU Mode is on.
  def getRapidsJARInfo: Seq[RapidsJarProfileResult] = {
    val allRows = apps.flatMap { app =>
      if (app.gpuMode) {
        // Look for rapids-4-spark and cuDF jar
        val rapidsJar = app.classpathEntries.filterKeys(_ matches ".*rapids-4-spark.*jar")
        val cuDFJar = app.classpathEntries.filterKeys(_ matches ".*cudf.*jar")
        val cols = (rapidsJar.keys ++ cuDFJar.keys).toSeq
        val rowsWithAppindex = cols.map(jar => RapidsJarProfileResult(app.index, jar))
        rowsWithAppindex
      } else {
        Seq.empty
      }
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.jar))
    } else {
      Seq.empty
    }
  }

  // get read data schema information
  def getDataSourceInfo: Seq[DataSourceProfileResult] = {
    val filtered = apps.filter(_.dataSourceInfo.size > 0)
    val allRows = filtered.flatMap { app =>
      app.dataSourceInfo.map { ds =>
        DataSourceProfileResult(app.index, ds.sqlID, ds.format, ds.location,
          ds.pushedFilters, ds.schema)
      }
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.sqlID, cols.location, cols.schema))
    } else {
      Seq.empty
    }
  }

  // get executor related information
  def getExecutorInfo: Seq[ExecutorInfoProfileResult] = {
    val allRows = apps.flatMap { app =>
      // first see if any executors have different resourceProfile ids
      val groupedExecs = app.executorIdToInfo.groupBy(_._2.resourceProfileId)
      groupedExecs.map { case (rpId, execs) =>
        val rp = app.resourceProfIdToInfo.get(rpId)
        val execMem = rp.map(_.executorResources.get(ResourceProfile.MEMORY)
          .map(_.amount).getOrElse(0L))
        val execCores = rp.map(_.executorResources.get(ResourceProfile.CORES)
          .map(_.amount).getOrElse(0L))
        val execGpus = rp.map(_.executorResources.get("gpu")
          .map(_.amount).getOrElse(0L))
        val taskCpus = rp.map(_.taskResources.get(ResourceProfile.CPUS)
          .map(_.amount).getOrElse(0.toDouble))
        val taskGpus = rp.map(_.taskResources.get("gpu").map(_.amount).getOrElse(0.toDouble))
        val execOffHeap = rp.map(_.executorResources.get(ResourceProfile.OFFHEAP_MEM)
          .map(_.amount).getOrElse(0L))

        val numExecutors = execs.size
        val exec = execs.head._2
        // We could print a lot more information here if we decided, more like the Spark UI
        // per executor info.
        ExecutorInfoProfileResult(app.index, rpId, numExecutors,
          exec.totalCores, exec.maxMemory, exec.totalOnHeap,
          exec.totalOffHeap, execMem, execGpus, execOffHeap, taskCpus, taskGpus)
      }
    }

    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.numExecutors))
    } else {
      Seq.empty
    }
  }

  // get job related information
  def getJobInfo: Seq[JobInfoProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.jobIdToInfo.map { case (jobId, j) =>
        JobInfoProfileResult(app.index, j.jobID, j.stageIds, j.sqlID, j.startTime, j.endTime)
      }
    }
    if (allRows.size > 0) {
      allRows.sortBy(cols => (cols.appIndex, cols.jobID))
    } else {
      Seq.empty
    }
  }

  def getSQLToStage: Seq[SQLStageInfoProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.aggregateSQLStageInfo
    }
    if (allRows.size > 0) {
      case class Reverse[T](t: T)
      implicit def ReverseOrdering[T: Ordering]: Ordering[Reverse[T]] =
        Ordering[T].reverse.on(_.t)

      // intentionally sort this table by the duration to be able to quickly
      // see the stage that took the longest
      allRows.sortBy(cols => (cols.appIndex, Reverse(cols.duration)))
    } else {
      Seq.empty
    }
  }

  // Print RAPIDS related or all Spark Properties
  // This table is inverse of the other tables where the row keys are
  // property keys and the columns are the application values. So
  // column1 would be all the key values for app index 1.
  def getProperties(rapidsOnly: Boolean): Seq[RapidsPropertyProfileResult] = {
    val outputHeaders = ArrayBuffer("propertyName")
    val props = HashMap[String, ArrayBuffer[String]]()
    var numApps = 0
    apps.foreach { app =>
      numApps += 1
      outputHeaders += s"appIndex_${app.index}"
      val propsToKeep = if (rapidsOnly) {
        app.sparkProperties.filterKeys { key =>
          key.startsWith("spark.rapids") || key.startsWith("spark.executorEnv.UCX") ||
            key.startsWith("spark.shuffle.manager") || key.equals("spark.shuffle.service.enabled")
        }
      } else {
        // remove the rapids related ones
        app.sparkProperties.filterKeys(key => !(key.contains("spark.rapids")))
      }
      CollectInformation.addNewProps(propsToKeep, props, numApps)
    }
    val allRows = props.map { case (k, v) => Seq(k) ++ v }.toSeq
    if (allRows.size > 0) {
      val resRows = allRows.map(r => RapidsPropertyProfileResult(r(0), outputHeaders, r))
      resRows.sortBy(cols => cols.key)
    } else {
      Seq.empty
    }
  }

  // Print SQL whole stage code gen mapping
  def getWholeStageCodeGenMapping: Seq[WholeStageCodeGenResults] = {
    val allWholeStages = apps.flatMap { app =>
      app.wholeStage
    }
    if (allWholeStages.size > 0) {
      allWholeStages.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID))
    } else {
      Seq.empty
    }
  }

  // Print SQL Plan Metrics
  def getSQLPlanMetrics: Seq[SQLAccumProfileResults] = {
    val acummsToReport = getSQLAccumulators.values.flatten.toSeq
    if (acummsToReport.size > 0) {
      acummsToReport.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID,
        cols.nodeName, cols.accumulatorId, cols.metricType))
    } else {
      Seq.empty
    }
  }

  def getSQLAccumulators(): Map[Int, Seq[SQLAccumProfileResults]] = {
    if (!sqlAccums.isDefined) {
      sqlAccums = Some(CollectInformation.generateSQLAccums(apps))
    }
    sqlAccums.get
  }
}

object CollectInformation extends Logging {

  private val SIZE_METRIC = "size"
  private val TIMING_METRIC = "timing"
  private val NS_TIMING_METRIC = "nsTiming"

  def generateSQLAccums(apps: Seq[ApplicationInfo]): Map[Int, Seq[SQLAccumProfileResults]] = {
    apps.map { app =>
      val metricResults = app.allSQLMetrics.flatMap { metric =>
        val sqlId = metric.sqlID
        val jobsForSql = app.jobIdToInfo.filter { case (_, jc) =>
          jc.sqlID.getOrElse(-1) == sqlId
        }
        val stageIdsForSQL = jobsForSql.flatMap(_._2.stageIds).toSeq
        val accumsOpt = app.taskStageAccumMap.get(metric.accumulatorId)
        val (maxValue, individualTaskMax, individualTaskMedian) = accumsOpt match {
          case Some(accums) =>
            val filtered = accums.filter { a =>
              stageIdsForSQL.contains(a.stageId)
            }
            val updateValues = filtered.map(_.update.getOrElse(0L))
            val (max, median) = if (updateValues.isEmpty) {
              (None, None)
            } else {
              Arrays.sort(updateValues.toArray)
              (Some(updateValues.max), Some(updateValues(updateValues.length / 2)))
            }
            val accumValues = filtered.map(_.value.getOrElse(0L))
            if (accumValues.isEmpty) {
              (None, max, median)
            } else {
              (Some(accumValues.max), max, median)
            }
          case None => (None, None, None)
        }

        // local mode driver gets updates
        val driverAccumsOpt = app.driverAccumMap.get(metric.accumulatorId)
        val driverMax = driverAccumsOpt match {
          case Some(accums) =>
            val filtered = accums.filter { a =>
              a.sqlID == sqlId
            }
            val accumValues = filtered.map(_.value)
            if (accumValues.isEmpty) {
              None
            } else {
              Some(accumValues.max)
            }
          case None =>
            None
        }

        val strFormat: Long => String = if (metric.metricType == SIZE_METRIC) {
          bytesToString
        } else if (metric.metricType == TIMING_METRIC) {
          msDurationToString
        } else if (metric.metricType == NS_TIMING_METRIC) {
          duration => msDurationToString(duration.nanos.toMillis)
        } else {
          // leave unformatted
          metricVal => metricVal.toString
        }

        if ((maxValue.isDefined) || (driverMax.isDefined)) {
          val max = Math.max(driverMax.getOrElse(0L), maxValue.getOrElse(0L))
          Some(SQLAccumProfileResults(app.index, metric.sqlID,
            metric.nodeID, metric.nodeName, metric.accumulatorId,
            metric.name, max, metric.metricType, metric.stageIds.mkString(","),
            strFormat(max),
            strFormat(individualTaskMax.getOrElse(0L)),
            strFormat(individualTaskMedian.getOrElse(0L))))
        } else {
          None
        }
      }
      (app.index, metricResults)
    }.toMap
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute
    val locale = Locale.US

    ms match {
      case t if t < second =>
        "%d ms".formatLocal(locale, t)
      case t if t < minute =>
        "%.1f s".formatLocal(locale, t.toFloat / second)
      case t if t < hour =>
        "%.1f m".formatLocal(locale, t.toFloat / minute)
      case t =>
        "%.2f h".formatLocal(locale, t.toFloat / hour)
    }
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    val PiB = 1L << 50
    val TiB = 1L << 40
    val GiB = 1L << 30
    val MiB = 1L << 20
    val KiB = 1L << 10

    if (size >= BigInt(1L << 11) * EiB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EiB) {
          (BigDecimal(size) / EiB, "EiB")
        } else if (size >= 2 * PiB) {
          (BigDecimal(size) / PiB, "PiB")
        } else if (size >= 2 * TiB) {
          (BigDecimal(size) / TiB, "TiB")
        } else if (size >= 2 * GiB) {
          (BigDecimal(size) / GiB, "GiB")
        } else if (size >= 2 * MiB) {
          (BigDecimal(size) / MiB, "MiB")
        } else if (size >= 2 * KiB) {
          (BigDecimal(size) / KiB, "KiB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

  // we don't really have metrics for how long shuffle read took
  val ioMetrics = immutable.HashSet("buffer time", "scan time", "size of files read",
    "GPU decode time", "number of files read", "static size of files read",
    "static number of files read",
    "shuffle write time", "fetch wait time", "job commit time", "GPU time",
    "task commit time", "remote blocks read", "local blocks read", "remote bytes read",
    "local bytes read", "concat batch time")
  val pureIOMetrics = immutable.HashSet("buffer time",
    "shuffle write time", "fetch wait time", "job commit time",
    "task commit time")
  def getIOMetrics(metrics: Map[Int, Seq[SQLAccumProfileResults]]):
    Map[Int, Seq[SQLAccumProfileResults]] = {
    metrics.map { case (appId, accumResults) =>
      val ioResults = accumResults.filter{ accum =>
        pureIOMetrics.contains(accum.name)
      }
      (appId, ioResults)
    }
  }

  def printSQLPlans(apps: Seq[ApplicationInfo], outputDir: String): Unit = {
    for (app <- apps) {
      val planFileWriter = new ToolTextFileWriter(s"$outputDir/${app.appId}",
        "planDescriptions.log", "SQL Plan")
      try {
        for ((sqlID, planDesc) <- app.physicalPlanDescription.toSeq.sortBy(_._1)) {
          planFileWriter.write("\n=============================\n")
          planFileWriter.write(s"Plan for SQL ID : $sqlID")
          planFileWriter.write("\n=============================\n")
          planFileWriter.write(planDesc)
        }
      } finally {
        planFileWriter.close()
      }
    }
  }

  // Update processed properties hashmap based on the new rapids related
  // properties for a new application passed in. This will updated the
  // processedProps hashmap in place to make sure each key in the hashmap
  // has the same number of elements in the ArrayBuffer.
  // It handles 3 cases:
  // 1) key in newRapidsRelated already existed in processedProps
  // 2) this app doesn't contain a key in newRapidsRelated that other apps had
  // 3) new key in newRapidsRelated that wasn't in processedProps for the apps already processed
  def addNewProps(newRapidsRelated: Map[String, String],
      processedProps: HashMap[String, ArrayBuffer[String]],
      numApps: Int): Unit = {

    val inter = processedProps.keys.toSeq.intersect(newRapidsRelated.keys.toSeq)
    val existDiff = processedProps.keys.toSeq.diff(inter)
    val newDiff = newRapidsRelated.keys.toSeq.diff(inter)

    // first update intersecting
    inter.foreach { k =>
      // note, this actually updates processProps as it goes
      val appVals = processedProps.getOrElse(k, ArrayBuffer[String]())
      appVals += newRapidsRelated.getOrElse(k, "null")
    }

    // this app doesn't contain a key that was in another app
    existDiff.foreach { k =>
      val appVals = processedProps.getOrElse(k, ArrayBuffer[String]())
      appVals += "null"
    }

    // this app contains a key not in other apps
    newDiff.foreach { k =>
      // we need to fill if some apps didn't have it
      val appVals = ArrayBuffer[String]()
      appVals ++= Seq.fill(numApps - 1)("null")
      appVals += newRapidsRelated.getOrElse(k, "null")

      processedProps.put(k, appVals)
    }
  }
}
