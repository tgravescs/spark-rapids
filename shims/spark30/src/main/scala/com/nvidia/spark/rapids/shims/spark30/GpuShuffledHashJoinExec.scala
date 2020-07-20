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

package com.nvidia.spark.rapids.shims.spark30

import com.nvidia.spark.rapids._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, ShuffledHashJoinExec}

case class GpuShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends GpuShuffledHashJoinExecBase with Logging {


  def getBuildSide: GpuBuildSide = {
    buildSide match {
      case BuildRight => GpuBuildRight
      case BuildLeft => GpuBuildLeft
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
}

object GpuShuffledHashJoinExec extends Logging {

  def createInstance(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      join: SparkPlan,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan): GpuShuffledHashJoinExec = {
    
    val buildSide: BuildSide = if (join.isInstanceOf[ShuffledHashJoinExec]) {
      join.asInstanceOf[ShuffledHashJoinExec].buildSide 
    } else {
      BuildRight
    }
    GpuShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right)
  }

}
