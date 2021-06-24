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

package com.nvidia.spark.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.EventLogPathProcessor
import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo

/**
 * A tool to analyze Spark event logs and determine if 
 * they might be a good fit for running on the GPU.
 */
object QualificationMain extends Logging {
  /**
   * Entry point from spark-submit running this as the driver.
   */
  def main(args: Array[String]) {
    val sparkSession = ProfileUtils.createSparkSession
    val exitCode = mainInternal(new QualificationArgs(args))
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(appArgs: QualificationArgs,
      writeOutput: Boolean = true,
      dropTempViews: Boolean = false): (Int, Seq[QualificationSummaryInfo]) = {

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val matchEventLogs = appArgs.matchEventLogs
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")
    val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

    val eventLogInfos = EventLogPathProcessor.processAllPaths(filterN.toOption,
      matchEventLogs.toOption, eventlogPaths)
    if (eventLogInfos.isEmpty) {
      logWarning("No event logs to process after checking paths, exiting!")
      return 0
    }

    val qual = new Qualification(outputDirectory)
    val res = qual.qualifyApps(eventLogInfos, numOutputRows)
    (0, res)
  }

}
