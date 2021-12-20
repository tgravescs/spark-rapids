/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

abstract class RapidsConvert[INPUT <: BASE, BASE, OUTPUT <: BASE,
  METATYPE <: RapidsMeta[INPUT, BASE, OUTPUT]]() {

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(meta: SparkPlanMeta[SparkPlan]): OUTPUT

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(meta: METATYPE): BASE = meta.wrapped

}


/**
 * Base class for metadata around `Scan`.
 */
abstract class ScanConvert[INPUT <: Scan]()
  extends RapidsConvert[INPUT, Scan, Scan, ScanMeta[INPUT]]() {
  }

/**
 * Metadata for `Scan` with no rule found
 */
final class RuleNotFoundScanConvert[INPUT <: Scan]()
  extends ScanConvert[INPUT]() {

  override def convertToGpu(meta: ScanMeta[INPUT]): Scan =
    throw new IllegalStateException("Cannot be converted to GPU")
}


abstract class PlanConvert[INPUT <: SparkPlan,
  META <: SparkPlanMeta[INPUT]](implicit tag: ClassTag[INPUT])
  extends RapidsConvert[INPUT, SparkPlan, GpuExec, META]() {

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  // def convertToGpu(wrapped: META): GpuExec

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  // def convertToCpu(meta: METATYPE): BASE = meta.wrapped

  override def convertToCpu(meta: META): SparkPlan = {
    meta.wrapped.withNewChildren(meta.childPlans.map(_.convertIfNeeded()))
  }

  def getClassFor: Class[_] = tag.runtimeClass

  /**
   * If this is enabled to be converted to a GPU version convert it and return the result, else
   * do what is needed to possibly convert the rest of the plan.
   */
  final def convertIfNeeded(meta: META): SparkPlan = {
    if (meta.shouldThisBeRemoved) {
      if (meta.childPlans.isEmpty) {
        throw new IllegalStateException("can't remove when plan has no children")
      } else if (meta.childPlans.size > 1) {
        throw new IllegalStateException("can't remove when plan has more than 1 child")
      }
      meta.childPlans.head.convertIfNeeded()
    } else {
      if (meta.canThisBeReplaced) {
        convertToGpu(meta)
      } else {
        convertToCpu(meta)
      }
    }
  }

}