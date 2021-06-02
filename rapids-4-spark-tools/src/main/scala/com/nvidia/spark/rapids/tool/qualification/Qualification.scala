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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Ranks the applications for GPU acceleration.
 */
object Qualification {

  def qualifyApps(apps: ArrayBuffer[ApplicationInfo]): DataFrame = {

    // The query generated for a lot of apps is to big and Spark analyzer
    // takes forever to handle. Break it up and do a few apps at a time and then
    // cache and union at the end. The results of each query per app is tiny so
    // caching should not use much memory.
    val groupedApps = apps.
      filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}")).grouped(4)
    val queries = groupedApps.map { group =>
      group.map("(" + _.qualificationDurationSumSQL + ")")
        .mkString(" union ")
    }
    val subqueryResults = queries.map { query =>
      apps.head.runQuery(query)
    }
    val cached = subqueryResults.map(_.cache()).toArray
    // materialize the cached datasets
    cached.foreach(_.count())
    val finalDf = if (cached.size > 1) {
      cached.reduce(_ union _).orderBy(col("dfRankTotal").desc, col("appDuration").desc)
    } else {
      cached.head.orderBy(col("dfRankTotal").desc, col("appDuration").desc)
    }
    finalDf
  }

  def writeQualification(apps: ArrayBuffer[ApplicationInfo],
      df: DataFrame, outputFileLoc: String, format: String): Unit = {
    // val fileWriter = apps.head.fileWriter
    val dfRenamed = apps.head.renameQualificationColumns(df)
    if (format.equals("csv")) {
      dfRenamed.repartition(1).write.mode("overwrite").csv("csvoutput")
    } else {
      val outputFilePath = new Path(outputFileLoc + "/tomtest")
      val fs = FileSystem.get(outputFilePath.toUri, new Configuration())
      val outFile = fs.create(outputFilePath)
      outFile.writeUTF(ToolUtils.showString(dfRenamed, 1000))
      outFile.flush()
      outFile.close()
    }

    // fileWriter.write("\n" + ToolUtils.showString(dfRenamed,
    //   apps(0).numOutputRows))
  }
}
