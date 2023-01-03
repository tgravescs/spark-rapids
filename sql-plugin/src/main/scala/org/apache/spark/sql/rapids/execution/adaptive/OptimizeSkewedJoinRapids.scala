/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.execution.adaptive

import scala.collection.mutable

import com.nvidia.spark.rapids.RapidsConf
import org.apache.commons.io.FileUtils

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ValidateRequirements}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A rule to optimize skewed joins to avoid straggler tasks whose share of data are significantly
 * larger than those of the rest of the tasks.
 *
 * The general idea is to divide each skew partition into smaller partitions and replicate its
 * matching partition on the other side of the join so that they can run in parallel tasks.
 * Note that when matching partitions from the left side and the right side both have skew,
 * it will become a cartesian product of splits from left and right joining together.
 *
 * For example, assume the Sort-Merge join has 4 partitions:
 * left:  [L1, L2, L3, L4]
 * right: [R1, R2, R3, R4]
 *
 * Let's say L2, L4 and R3, R4 are skewed, and each of them get split into 2 sub-partitions. This
 * is scheduled to run 4 tasks at the beginning: (L1, R1), (L2, R2), (L3, R3), (L4, R4).
 * This rule expands it to 9 tasks to increase parallelism:
 * (L1, R1),
 * (L2-1, R2), (L2-2, R2),
 * (L3, R3-1), (L3, R3-2),
 * (L4-1, R4-1), (L4-2, R4-1), (L4-1, R4-2), (L4-2, R4-2)
 */
case class OptimizeSkewedJoinRapids(ensureRequirements: EnsureRequirements)
  extends Rule[SparkPlan] {

  logWarning("optimized skew join installed TOM")

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * SKEW_JOIN_SKEWED_PARTITION_FACTOR and also larger than
   * SKEW_JOIN_SKEWED_PARTITION_THRESHOLD. Thus we pick the larger one as the skew threshold.
   */
  def getSkewThreshold(medianSize: Long): Long = {
    conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
      (medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR)).toLong)
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * advisory partition size if avg size is smaller than it.
   */
  private def targetSize(sizes: Array[Long], skewThreshold: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filter(_ <= skewThreshold)
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }

  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  private def getSizeInfo(medianSize: Long, sizes: Array[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin shuffled join (smj and shj).
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle read that loads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle read that loads partition0 3 times by
   *    3 tasks separately.
   */
  private def tryOptimizeJoinChildren(
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec,
      joinType: JoinType): Option[(SparkPlan, SparkPlan)] = {
    val canSplitLeft = canSplitLeftSide(joinType)
    val canSplitRight = canSplitRightSide(joinType)
    if (!canSplitLeft && !canSplitRight) return None

    val leftSizes = left.mapStats.get.bytesByPartitionId
    val rightSizes = right.mapStats.get.bytesByPartitionId
    assert(leftSizes.length == rightSizes.length)
    val numPartitions = leftSizes.length
    // We use the median size of the original shuffle partitions to detect skewed partitions.
    val leftMedSize = Utils.median(leftSizes, false)
    val rightMedSize = Utils.median(rightSizes, false)
    logDebug(
      s"""
         |Optimizing skewed join.
         |Left side partitions size info:
         |${getSizeInfo(leftMedSize, leftSizes)}
         |Right side partitions size info:
         |${getSizeInfo(rightMedSize, rightSizes)}
      """.stripMargin)

    val leftSkewThreshold = getSkewThreshold(leftMedSize)
    val rightSkewThreshold = getSkewThreshold(rightMedSize)
    val leftTargetSize = targetSize(leftSizes, leftSkewThreshold)
    val rightTargetSize = targetSize(rightSizes, rightSkewThreshold)

    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    var numSkewedRight = 0
    for (partitionIndex <- 0 until numPartitions) {
      val leftSize = leftSizes(partitionIndex)
      val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
      val rightSize = rightSizes(partitionIndex)
      val isRightSkew = canSplitRight && rightSize > rightSkewThreshold
      val leftNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, leftSize))
      val rightNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, rightSize))

      val leftParts = if (isLeftSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          left.mapStats.get.shuffleId, partitionIndex, leftTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Left side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(leftSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedLeft += 1
        }
        skewSpecs.getOrElse(leftNoSkewPartitionSpec)
      } else {
        leftNoSkewPartitionSpec
      }

      val rightParts = if (isRightSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          right.mapStats.get.shuffleId, partitionIndex, rightTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Right side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(rightSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedRight += 1
        }
        skewSpecs.getOrElse(rightNoSkewPartitionSpec)
      } else {
        rightNoSkewPartitionSpec
      }

      for {
        leftSidePartition <- leftParts
        rightSidePartition <- rightParts
      } {
        leftSidePartitions += leftSidePartition
        rightSidePartitions += rightSidePartition
      }
    }
    logDebug(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
    if (numSkewedLeft > 0 || numSkewedRight > 0) {
      Some((
        SkewJoinChildWrapper(AQEShuffleReadExec(left, leftSidePartitions.toSeq)),
        SkewJoinChildWrapper(AQEShuffleReadExec(right, rightSidePartitions.toSeq))
      ))
    } else {
      None
    }
  }



  private def tryOptimizeBroadcastJoinChildren(
      left: SparkPlan,
      right: ShuffleQueryStageExec,
      joinType: JoinType,
      buildSide: BuildSide): (Option[SparkPlan], Option[SparkPlan]) = {
    // buildSide should be the broadcast side, so check to see if split
    // the other side
    val canSplitRight = canSplitRightSide(joinType)
    val canSplitLeft = canSplitLeftSide(joinType)
    if (!canSplitLeft && !canSplitRight) return (None, None)

    logWarning(
      s"""
         |Broadcast Optimizing skewed join.
      """.stripMargin)

    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedRight = 0
    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    buildSide match {
      case BuildLeft =>
        val rightSizes = right.mapStats.get.bytesByPartitionId
        val numPartitions = rightSizes.length
        // We use the median size of the original shuffle partitions to detect skewed partitions.
        val rightMedSize = Utils.median(rightSizes, false)
        val rightSkewThreshold = getSkewThreshold(rightMedSize)
        val rightTargetSize = targetSize(rightSizes, rightSkewThreshold)

        for (partitionIndex <- 0 until numPartitions) {
          val rightSize = rightSizes(partitionIndex)
          val isRightSkew = canSplitRight && rightSize > rightSkewThreshold
          val rightNoSkewPartitionSpec =
            Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, rightSize))

          val rightParts = if (isRightSkew) {
            val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
              right.mapStats.get.shuffleId, partitionIndex, rightTargetSize)
            if (skewSpecs.isDefined) {
              logWarning(s"Right side partition $partitionIndex " +
                s"(${FileUtils.byteCountToDisplaySize(rightSize)}) is skewed, " +
                s"split it into ${skewSpecs.get.length} parts.")
              numSkewedRight += 1
            }
            skewSpecs.getOrElse(rightNoSkewPartitionSpec)
          } else {
            rightNoSkewPartitionSpec
          }

          for {
            rightSidePartition <- rightParts
          } {
            rightSidePartitions += rightSidePartition
          }
        }
      case _ => (None, None)

    }

    logWarning(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
    if (numSkewedLeft > 0) {
      logWarning("skew left number " + leftSidePartitions.size)

      (Some(SkewJoinChildWrapper(AQEShuffleReadExec(left, leftSidePartitions.toSeq))),
       None)
    } else if (numSkewedRight > 0) {
      logWarning("skew right number " + rightSidePartitions.size)
      (None,
        Some(SkewJoinChildWrapper(AQEShuffleReadExec(right, rightSidePartitions.toSeq))))
    } else {
      (None, None)
    }
  }

  private def tryOptimizeBroadcastJoinChildren2(
      left: ShuffleQueryStageExec,
      right: SparkPlan,
      joinType: JoinType,
      buildSide: BuildSide): (Option[SparkPlan], Option[SparkPlan]) = {
    // buildSide should be the broadcast side, so check to see if split
    // the other side
    val canSplitRight = canSplitRightSide(joinType)
    val canSplitLeft = canSplitLeftSide(joinType)
    if (!canSplitLeft && !canSplitRight) return (None, None)

    logWarning(
      s"""
         |Broadcast Optimizing skewed join.
    """.stripMargin)

    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedRight = 0
    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    buildSide match {
      case BuildRight =>
      val leftSizes = left.mapStats.get.bytesByPartitionId
      val numPartitions = leftSizes.length
      // We use the median size of the original shuffle partitions to detect skewed partitions.
      val leftMedSize = Utils.median(leftSizes, false)
      val leftSkewThreshold = getSkewThreshold(leftMedSize)
      val leftTargetSize = targetSize(leftSizes, leftSkewThreshold)

      for (partitionIndex <- 0 until numPartitions) {
        val leftSize = leftSizes(partitionIndex)
        val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
        val leftNoSkewPartitionSpec =
          Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, leftSize))

        val leftParts = if (isLeftSkew) {
          val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
            left.mapStats.get.shuffleId, partitionIndex, leftTargetSize)
          if (skewSpecs.isDefined) {
            logInfo(s"Left side partition $partitionIndex " +
              s"(${FileUtils.byteCountToDisplaySize(leftSize)}) is skewed, " +
              s"split it into ${skewSpecs.get.length} parts.")
            numSkewedLeft += 1
          }
          skewSpecs.getOrElse(leftNoSkewPartitionSpec)
        } else {
          leftNoSkewPartitionSpec
        }

        for {
          leftSidePartition <- leftParts
        } {
          leftSidePartitions += leftSidePartition
        }
      }
    case _ => (None, None)


    }

    logInfo(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
    if (numSkewedLeft > 0) {
      (Some(SkewJoinChildWrapper(AQEShuffleReadExec(left, leftSidePartitions.toSeq))),
        None)
    } else if (numSkewedRight > 0) {
      (None,
        Some(SkewJoinChildWrapper(AQEShuffleReadExec(right, rightSidePartitions.toSeq))))
    } else {
      (None, None)
    }
  }


  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = {
    logWarning("in optimize skew join plan before is: " + plan)
    plan.transformUp {
      /*
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleQueryStageExec), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleQueryStageExec), _), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          smj.copy(
            left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      }.getOrElse(smj)

    case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
        ShuffleStage(left: ShuffleQueryStageExec),
        ShuffleStage(right: ShuffleQueryStageExec), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          shj.copy(left = newLeft, right = newRight, isSkewJoin = true)
      }.getOrElse(shj)
      */

      /*
    case bhj @ BroadcastHashJoinExec(_, _, joinType, buildSide, _, left,
      ShuffleStage(right: ShuffleQueryStageExec), false, _) =>
      logWarning("in bhj check skewed")
      if (conf.getConf(SQLConf.SKEW_JOIN_BROADCAST_ENABLED)) {
        val (newLeft, newRight) = tryOptimizeBroadcastJoinChildren(left, right, joinType, buildSide)
        if (newLeft.isDefined && buildSide == BuildLeft) {
          val res = bhj.copy(left = newLeft.get, right = newRight.get, isSkewed = true)
          logWarning("left skewed: " + res)
          res
        } else if (newRight.isDefined) {
          logWarning("right skewed")
          bhj.copy(right = newRight.get)
        } else {
          logWarning("No skewed BHJ")
          bhj
        }
      } else {
        logWarning("in else")
        bhj
      }

 */
    case bhjno @ BroadcastHashJoinExec(_, _, joinType, buildSide, _,
    left, right, _) =>
      logWarning("build side is: " + buildSide)
      logWarning("broadcasthash join parameters are left: " + left)
      logWarning("broadcasthash join parameters are right: " + right)

      val rapidsConf = new RapidsConf(plan.conf)

      if (rapidsConf.skewJoinBroadcastEnabled) {
        val (newLeft, newRight) = if (buildSide == BuildRight) {
          if (left.isInstanceOf[ShuffleQueryStageExec]) {
            if (left.asInstanceOf[ShuffleQueryStageExec].isMaterialized) {
              logWarning("RIGHT is broadcast")
              // right side is broadcast side, left is shuffle
              val (newLeft, newRight) = tryOptimizeBroadcastJoinChildren2(left.asInstanceOf[ShuffleQueryStageExec],
                right, joinType, buildSide)
              (newLeft, newRight)
            } else {
              logWarning("LEFT IS NOT MATERIALIZED class is: " + left.getClass)
              // not materialized but we have to change the broadcast number of partitions and
              // the real exchange should be materialized above it so we should be able to compute
              (None, None)
            }
          } else {
            logWarning("LEFT IS NOT MATERIALIZED class is: " + left.getClass)
            // not materialized but we have to change the broadcast number of partitions and
            // the real exchange should be materialized above it so we should be able to compute
            (None, None)
          }
        } else {
          logWarning("in else builsside left")
          if (right.isInstanceOf[ShuffleQueryStageExec]) {
            if (right.asInstanceOf[ShuffleQueryStageExec].isMaterialized) {
              val (newLeft, newRight) = tryOptimizeBroadcastJoinChildren(left,
                right.asInstanceOf[ShuffleQueryStageExec], joinType, buildSide)
              (newLeft, newRight)
            } else {
              (None, None)
            }
          } else {
            (None, None)
          }
        }

        if (newLeft.isDefined) {
          bhjno.copy(left = newLeft.get) // , isSkewed = true)
        } else if (newRight.isDefined) {
          bhjno.copy(right = newRight.get) //, isSkewed = true)
        } else {
          bhjno
        }
      } else {
        bhjno
      }

    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    logWarning("in optimize skew join tom")

    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }

    // We try to optimize every skewed sort-merge/shuffle-hash joins in the query plan. If this
    // introduces extra shuffles, we give up the optimization and return the original query plan, or
    // accept the extra shuffles if the force-apply config is true.
    // TODO: It's possible that only one skewed join in the query plan leads to extra shuffles and
    //       we only need to skip optimizing that join. We should make the strategy smarter here.
    val optimized = optimizeSkewJoin(plan)
    val requirementSatisfied = if (ensureRequirements.requiredDistribution.isDefined) {
      ValidateRequirements.validate(optimized, ensureRequirements.requiredDistribution.get)
    } else {
      ValidateRequirements.validate(optimized)
    }
    val res = if (requirementSatisfied) {
      optimized.transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else if (conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)) {
      ensureRequirements.apply(optimized).transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else {
      plan
    }
    res
  }

  object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec =>
        if (s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS) {
          Some(s)
        } else {
          None
        }

      case _ => None
    }
  }

}

// After optimizing skew joins, we need to run EnsureRequirements again to add necessary shuffles
// caused by skew join optimization. However, this shouldn't apply to the sub-plan under skew join,
// as it's guaranteed to satisfy distribution requirement.
case class SkewJoinChildWrapper(plan: SparkPlan) extends LeafExecNode with Logging {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = {
    plan.outputPartitioning
  }
  override def outputOrdering: Seq[SortOrder] = {
    plan.outputOrdering
  }
}
