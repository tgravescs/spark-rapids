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

package com.nvidia.spark.rapids.tool.profiling

import java.io.{File, FileWriter}

import org.scalatest.FunSuite

import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.profiling._

class QualificationSuite extends FunSuite {

  private val sparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  private val expRoot = ProfilingTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ProfilingTestUtils.getTestResourcePath("spark-events-qualification")

  private def runQualificationTest(eventLogs: Array[String], expectFileName: String) = {
    TrampolineUtil.withTempDir { outpath =>
      val resultExpectation = new File(expRoot, expectFileName)
      val appArgs = new QualificationArgs(Array(
        "--output-directory",
        outpath.getAbsolutePath()) ++ eventLogs
      )

      val (exit, dfQualOpt) =
        QualificationMain.mainInternal(sparkSession, appArgs, writeOutput=false)
      assert(exit == 0)
      // make sure to change null value so empty strings don't show up as nulls
      val dfExpect = sparkSession.read.option("header", "true").
        option("nullValue", "\"null\"").csv(resultExpectation.getPath)
      val diffCount = dfQualOpt.map { dfQual =>
        dfQual.except(dfExpect).union(dfExpect.except(dfExpect)).count
      }.getOrElse(-1)

      assert(diffCount == 0)
    }
  }

  test("test udf event logs") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    runQualificationTest(logFiles, "qual_test_simple_expectation.csv")
  }

  test("test missing sql end") {
    TrampolineUtil.withTempDir { outpath =>
      // val logfiles = Array(s"$logDir/join_missing_sql_end")
      // runQualificationTest(logFiles, "qual_test_missing_sql_end_expectation.csv")
      val resultExpectation = new File(expRoot, "qual_test_missing_sql_end_expectation.csv")

      val appArgs = new QualificationArgs(Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        s"$logDir/join_missing_sql_end"
      ))

      val (exit, dfQualOpt) = QualificationMain.mainInternal(sparkSession, appArgs, writeOutput=false)

      // make sure to change null value so empty strings don't show up as nulls
      val dfExpect = sparkSession.read.option("header", "true").option("nullValue", "-").csv(resultExpectation.getPath)
      dfExpect.write.csv("dfexpect")
      dfQualOpt.get.repartition(1).write.csv("dfqual")
      val diffCount = dfQualOpt.map { dfQual =>
        dfQual.show()
        dfExpect.show()
         dfQual.except(dfExpect).show()
         dfExpect.except(dfExpect).show()
        dfQual.except(dfExpect).union(dfExpect.except(dfExpect)).count
      }.getOrElse(-1)

      assert(diffCount == 0)
    }
  }
}
