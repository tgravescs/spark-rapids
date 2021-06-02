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

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Ranks the applications for GPU acceleration.
 */
object Qualification {

  def qualifyApps(apps: ArrayBuffer[ApplicationInfo]): DataFrame = {
    val query = apps
      .filter(p => p.allDataFrames.contains(s"sqlDF_${p.index}"))
      .map("(" + _.qualificationDurationSumSQL + ")")
      .mkString(" union ")
    val df = apps.head.runQuery(query + " order by dfRankTotal desc, appDuration desc")
    df
  }

  def writeQualification(apps: ArrayBuffer[ApplicationInfo], df: DataFrame): Unit = {
    // val fileWriter = apps.head.fileWriter
    val dfRenamed = apps.head.renameQualificationColumns(df)
    dfRenamed.repartition(1).write.csv("csvoutput")
    // fileWriter.write("\n" + ToolUtils.showString(dfRenamed,
    //   apps(0).numOutputRows))
  }
}
