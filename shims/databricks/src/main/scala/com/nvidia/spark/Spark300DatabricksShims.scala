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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuBuildLeft, GpuBuildRight, GpuBuildSide}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec



class Spark300DatabricksShims extends SparkShims with Logging {

  def getBuildSide(join: ShuffledHashJoinExec)): GpuBuildSide = {
    val buildSide = join.buildSide
    buildSide match {
      case e: buildSide.type if e.toString.contains("BuildRight") => {
        logInfo("Tom buildright " + e)
        GpuBuildRight
      }
      case l: buildSide.type if l.toString.contains("BuildLeft") => {
        logInfo("Tom buildleft "+ l)
        GpuBuildLeft
      }
      case _ => throw new Exception("unknown buildSide Type")
    }
  }

  def getBuildSide(join: BroadcastNestedLoopJoinExec)): GpuBuildSide = {
    val buildSide = join.buildSide
    buildSide match {
      case e: buildSide.type if e.toString.contains("BuildRight") => {
        logInfo("bnlje Tom buildright " + e)
        GpuBuildRight
      }
      case l: buildSide.type if l.toString.contains("BuildLeft") => {
        logInfo("bnlje Tom buildleft "+ l)
        GpuBuildLeft
      }
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
}

class GpuShimBuilSideHashJoin {
  def buildSide: BuildSide

}

