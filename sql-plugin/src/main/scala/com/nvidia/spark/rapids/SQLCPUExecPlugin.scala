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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.AQEUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.rapids.execution.adaptive.OptimizeSkewedJoinRapids

/**
 * Extension point to enable CPU AQE optimizations.
 */
class SQLCPUExecPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectQueryStagePrepRule(queryStagePrepOverrides)
  }

  private def queryStagePrepOverrides(sparkSession: SparkSession): Rule[SparkPlan] = {
    CpuQueryStagePrepOverrides()
  }
}

case class CpuQueryStagePrepOverrides() extends Rule[SparkPlan] with Logging {
  override def apply(sparkPlan: SparkPlan): SparkPlan = GpuOverrideUtil.tryOverride { plan =>
    val requiredDistribution: Option[Distribution] = AQEUtils.getRequiredDistribution(sparkPlan)
    // TODO - HOW DO WE GET isSubQuery???
    /* if (isSubquery) {
    // Subquery output does not need a specific output partitioning.
    Some(UnspecifiedDistribution)
  } else {
    AQEUtils.getRequiredDistribution(sparkPlan)
  }

     */
    logWarning("before running optimize skew join rapids")
    val ensureRequirements =
      EnsureRequirements(requiredDistribution.isDefined, requiredDistribution)
    val optimizedPlan = OptimizeSkewedJoinRapids(ensureRequirements).apply(plan)
    logWarning("after  running optimize skew join rapids plan is: " + optimizedPlan)

    optimizedPlan
  }(sparkPlan)
}