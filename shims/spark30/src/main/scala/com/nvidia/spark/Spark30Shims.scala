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

import java.time.ZoneId

import com.nvidia.spark.rapids._
import org.apache.spark.sql.rapids._

//import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}



class Spark30Shims extends SparkShims with Logging {

  def getExecs: Seq[ExecRule[_ <: SparkPlan]] = {
    Seq(
   GpuOverrides.exec[SortMergeJoinExec](
      "Sort merge join, replacing with shuffled hash join",
      (join, conf, p, r) => new GpuHashJoinBaseMeta(join, conf, p, r) {

        val leftKeys: Seq[BaseExprMeta[_]] =
          join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val rightKeys: Seq[BaseExprMeta[_]] =
          join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val condition: Option[BaseExprMeta[_]] = join.condition.map(
          GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition
        override val childPlans: Seq[SparkPlanMeta[_]] =
          join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {

    // Use conditions from Hash Join
    GpuHashJoin30.tagJoin(this, join.joinType, join.leftKeys, join.rightKeys, join.condition)

    if (!conf.enableReplaceSortMergeJoin) {
      willNotWorkOnGpu(s"Not replacing sort merge join with hash join, " +
        s"see ${RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key}")
    }

    // make sure this is last check - if this is SortMergeJoin, the children can be Sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // ShuffleHashJoin
    if (canThisBeReplaced) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu(s"can't replace sortMergeJoin because one of the SortExec's before " +
              s"can't be replaced.")
          } else {
            plan.shouldBeRemoved("removing SortExec as part replacing sortMergeJoin with " +
              s"shuffleHashJoin")
          }
        }
      }
    }
  }
 override def convertToGpu(): GpuExec = {
    GpuShuffledHashJoinExec30(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      BuildRight,
      condition.map(_.convertToGpu()),
      childPlans(0).convertIfNeeded(),
      childPlans(1).convertIfNeeded())
  }
      }),
    GpuOverrides.exec[ShuffledHashJoinExec](
      "Implementation of join using hashed shuffled data",
      (join, conf, p, r) => new GpuShuffledHashJoinMeta30(join, conf, p, r) {
         override val childPlans: Seq[SparkPlanMeta[_]] =
           join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))

        val leftKeys: Seq[BaseExprMeta[_]] =
          join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val rightKeys: Seq[BaseExprMeta[_]] =
          join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition
      }),
   GpuOverrides.exec[BroadcastHashJoinExec](
      "Implementation of join using broadcast data",
      (join, conf, p, r) => new GpuBroadcastHashJoinMeta30(join, conf, p, r) {
        val leftKeys: Seq[BaseExprMeta[_]] =
          join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val rightKeys: Seq[BaseExprMeta[_]] =
          join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childPlans: Seq[SparkPlanMeta[_]] =
    join.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
       override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition
      }),
    )
  }

  def getExprs: Seq[ExprRule[_ <: Expression]] = {
    Seq(
    GpuOverrides.expr[TimeSub](
      "Subtracts interval from timestamp",
      (a, conf, p, r) => new WrapBinaryExprMeta[TimeSub](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.interval match {
            case Literal(intvl: CalendarInterval, DataTypes.CalendarIntervalType) =>
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            case _ =>
              willNotWorkOnGpu("only literals are supported for intervals")
          }
          if (ZoneId.of(a.timeZoneId.get).normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
            willNotWorkOnGpu("Only UTC zone id is supported")
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimeSub(lhs, rhs)
      }
    )
    )
  }


  def getBuildSide(join: ShuffledHashJoinExec): GpuBuildSide = {
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

  def getBuildSide(join: BroadcastNestedLoopJoinExec): GpuBuildSide = {
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
  def getBuildSide(join: BroadcastHashJoinExec): GpuBuildSide = {
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

