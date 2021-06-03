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

import java.io.FileWriter

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.rapids.tool.profiling._

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
    val (exitCode, optDf) = mainInternal(sparkSession, new QualificationArgs(args))
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(sparkSession: SparkSession, appArgs: QualificationArgs,
      writeOutput: Boolean = true): (Int, Option[DataFrame]) = {

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")

    // Convert the input path string to Path(s)
    val allPaths: ArrayBuffer[Path] = ArrayBuffer[Path]()
    for (pathString <- eventlogPaths) {
      val paths = ProfileUtils.stringToPath(pathString)
      if (paths.nonEmpty) {
        allPaths ++= paths
      }
    }
    val df = Qualification.qualifyApps(allPaths,
      appArgs.numOutputRows.getOrElse(1000), sparkSession,
      appArgs.includeExecCpuPercent.getOrElse(false))
    logWarning("done qualify, before write")

    if (writeOutput) {
      Qualification.writeQualification(df, outputDirectory, appArgs.outputFormat.getOrElse("csv"))
    }

    (0, Some(df))
  }

  def logApplicationInfo(app: ApplicationInfo) = {
      logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }
}