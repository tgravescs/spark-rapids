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

import org.rogach.scallop.{ScallopConf, ScallopOption}

class QualificationArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""
RAPIDS Accelerator for Apache Spark qualification tool

Example:

# Input 1 or more event logs from local path:
./bin/spark-submit --class com.nvidia.spark.rapids.tool.qualification.QualificationMain
rapids-4-spark-tools_2.12-<version>.jar /path/to/eventlog1 /path/to/eventlog2

# Specify a directory of event logs from local path:
./bin/spark-submit --class com.nvidia.spark.rapids.tool.qualification.QualificationMain
rapids-4-spark-tools_2.12-<version>.jar /path/to/DirOfManyEventLogs

# If any event log is from S3:
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
./bin/spark-submit --class com.nvidia.spark.rapids.tool.qualification.QualificationMain
rapids-4-spark-tools_2.12-<version>.jar s3a://<BUCKET>/eventlog1 /path/to/eventlog2

# Change output directory to /tmp
./bin/spark-submit --class com.nvidia.spark.rapids.tool.qualification.QualificationMain
rapids-4-spark-tools_2.12-<version>.jar -o /tmp /path/to/eventlog1

For usage see below:
    """)

  val outputDirectory: ScallopOption[String] =
    opt[String](required = false,
      descr = "Output directory. Default is current directory",
      default = Some("."))
  val outputFormat: ScallopOption[String] =
    opt[String](required = false,
      descr = "Output format, supports csv and text. Default is csv",
      default = Some("csv"))
  val eventlog: ScallopOption[List[String]] =
    trailArg[List[String]](required = true,
      descr = "Event log filenames(space separated). " +
          "eg: s3a://<BUCKET>/eventlog1 /path/to/eventlog2")
  val numOutputRows: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Number of output rows for each Application. Default is 1000")
  val includeExecCpuPercent: ScallopOption[Boolean] =
    opt[Boolean](
      required = false,
      default = Some(false),
      descr = "Include the executor CPU time percent. It will take longer with this option.")
  verify()
}