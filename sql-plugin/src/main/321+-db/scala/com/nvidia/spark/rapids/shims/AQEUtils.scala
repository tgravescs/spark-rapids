/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashPartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CollectMetricsExec, DeserializeToObjectExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_COL, REPARTITION_BY_NUM, ShuffleExchangeExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf

/** Utility methods for manipulating Catalyst classes involved in Adaptive Query Execution */
object AQEUtils {
  /** Return a new QueryStageExec reuse instance with updated output attributes */
  def newReuseInstance(sqse: ShuffleQueryStageExec, newOutput: Seq[Attribute]): QueryStageExec = {
    val reusedExchange = ReusedExchangeExec(newOutput, sqse.shuffle)
    ShuffleQueryStageExec(sqse.id, reusedExchange, sqse.originalPlan, sqse.isSparkExchange)
  }

  // Databricks 10.4 has an issue where if you turn off AQE it can still use it for
  // certain operations. This causes issues with the plugin so this is to work around
  // that.
  def isAdaptiveExecutionSupportedInSparkVersion(conf: SQLConf): Boolean = {
    conf.getConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
  }

  // Analyze the given plan and calculate the required distribution of this plan w.r.t. the
  // user-specified repartition.
  def getRequiredDistribution(p: SparkPlan): Option[Distribution] = p match {
    // User-specified repartition is only effective when it's the root node, or under
    // Project/Filter/LocalSort/CollectMetrics.
    // Note: we only care about `HashPartitioning` as `EnsureRequirements` can only optimize out
    // user-specified repartition with `HashPartitioning`.
    case ShuffleExchangeExec(h: HashPartitioning, _, shuffleOrigin)
      if shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM =>
      val numPartitions = if (shuffleOrigin == REPARTITION_BY_NUM) {
        Some(h.numPartitions)
      } else {
        None
      }
      Some(ClusteredDistribution(h.expressions, requiredNumPartitions = numPartitions))
    case f: FilterExec => getRequiredDistribution(f.child)
    case s: SortExec if !s.global => getRequiredDistribution(s.child)
    case c: CollectMetricsExec => getRequiredDistribution(c.child)
    case d: DeserializeToObjectExec => getRequiredDistribution(d.child)
    case p: ProjectExec =>
      getRequiredDistribution(p.child).flatMap {
        case h: ClusteredDistribution =>
          if (h.clustering.forall(e => p.projectList.exists(_.semanticEquals(e)))) {
            Some(h)
          } else {
            // It's possible that the user-specified repartition is effective but the output
            // partitioning is not retained, e.g. `df.repartition(a, b).select(c)`. We can't
            // handle this case with required distribution. Here we return None and later on
            // `EnsureRequirements` will skip optimizing out the user-specified repartition.
            None
          }
        case other => Some(other)
      }
    case _ => Some(UnspecifiedDistribution)
  }
}
