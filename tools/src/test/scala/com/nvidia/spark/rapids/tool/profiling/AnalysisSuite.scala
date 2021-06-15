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

import java.io.File

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class AnalysisSuite extends FunSuite {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test sqlMetricsAggregation simple") {
    testSqlMetricsAggregation(Array(s"$logDir/rapids_join_eventlog.zstd"),
      "rapids_join_eventlog_sqlmetricsagg_expectation.csv",
      "rapids_join_eventlog_jobandstagemetrics_expectation.csv")
  }

  test("test sqlMetricsAggregation second single app") {
    testSqlMetricsAggregation(Array(s"$logDir/rapids_join_eventlog2.zstd"),
      "rapids_join_eventlog_sqlmetricsagg2_expectation.csv",
      "rapids_join_eventlog_jobandstagemetrics2_expectation.csv")
  }

  test("test sqlMetricsAggregation 2 combined") {
    testSqlMetricsAggregation(
      Array(s"$logDir/rapids_join_eventlog.zstd", s"$logDir/rapids_join_eventlog2.zstd"),
      "rapids_join_eventlog_sqlmetricsaggmulti_expectation.csv",
      "rapids_join_eventlog_jobandstagemetricsmulti_expectation.csv")
  }

  private def testSqlMetricsAggregation(logs: Array[String], expectFile: String,
      expectFileJS: String): Unit = {
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == logs.size)
    val analysis = new Analysis(apps, None)

    val actualDf = analysis.sqlMetricsAggregation()
    val resultExpectation = new File(expRoot,expectFile)
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(actualDf, dfExpect)

    val actualDfJS = analysis.jobAndStageMetricsAggregation()
    val resultExpectationJS = new File(expRoot, expectFileJS)
    val dfExpectJS = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectationJS.getPath())
    ToolTestUtils.compareDataFrames(actualDfJS, dfExpectJS)
  }

  test("test sqlMetrics duration and execute cpu time") {
    val logs = Array(s"$logDir/rp_sql_eventlog.zstd")
    val expectFile = "rapids_duration_and_cpu_expectation.csv"

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val analysis = new Analysis(apps, None)
    val sqlAggMetricsDF = analysis.sqlMetricsAggregation()
    sqlAggMetricsDF.createOrReplaceTempView("sqlAggMetricsDF")
    val actualDf = analysis.sqlMetricsAggregationDurationAndCpuTime()
    val resultExpectation = new File(expRoot, expectFile)
    val schema = new StructType()
      .add("appIndex",IntegerType,true)
      .add("appID",StringType,true)
      .add("sqlID",LongType,true)
      .add("sqlDuration",LongType,true)
      .add("containsDataset",BooleanType,true)
      .add("appDuration",LongType,true)
      .add("problematic",StringType,true)
      .add("executorCpuTime",DoubleType,true)

    val dfExpect = sparkSession.read.option("header", "true").option("nullValue", "-")
      .schema(schema).csv(resultExpectation.getPath())

    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }

  test("test shuffleSkewCheck empty") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog.zstd"), sparkSession)
    assert(apps.size == 1)

    val analysis = new Analysis(apps, None)
    val actualDf = analysis.shuffleSkewCheckSingleApp(apps.head)
    assert(actualDf.count() == 0)
  }

  test("test contains dataset false") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val logs = Array(s"$qualLogDir/nds_q86_test.zstd")

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val analysis = new Analysis(apps, None)
    val sqlAggMetricsDF = analysis.sqlMetricsAggregation()
    sqlAggMetricsDF.createOrReplaceTempView("sqlAggMetricsDF")
    val actualDf = analysis.sqlMetricsAggregationDurationAndCpuTime()

    val rows = actualDf.collect()
    assert(rows.length === 25)
    def fieldIndex(name: String) = actualDf.schema.fieldIndex(name)
    rows.foreach { row =>
      assert(row.getBoolean(fieldIndex("Contains Dataset Op")) == false)
    }
  }
}
