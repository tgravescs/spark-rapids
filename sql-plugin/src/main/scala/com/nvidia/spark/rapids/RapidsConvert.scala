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

import java.time.ZoneId

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryExpression, ComplexTypeMergingExpression, Expression, QuaternaryExpression, String2TrimExpression, TernaryExpression, UnaryExpression, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.rapids.{CpuToGpuAggregateBufferConverter, GpuToCpuAggregateBufferConverter}
import org.apache.spark.sql.types.DataType

/**
 * Holds metadata about a stage in the physical plan that is separate from the plan itself.
 * This is helpful in deciding when to replace part of the plan with a GPU enabled version.
 *
 * @param wrapped what we are wrapping
 * @param conf the config
 * @param parent the parent of this node, if there is one.
 * @param rule holds information related to the config for this object, typically this is the rule
 *          used to wrap the stage.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam OUTPUT when converting to a GPU enabled version of the plan, the generic base
 *                    type for all GPU enabled versions.
 */
abstract class RapidsConvert[INPUT <: BASE, BASE, OUTPUT <: BASE, METATYPE <: RapidsMeta[INPUT, BASE, OUTPUT]](
    val meta: METATYPE,
    rule: DataFromReplacementRule) {

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(): OUTPUT

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(): BASE = meta.wrapped

}


/**
 * Base class for metadata around `Scan`.
 */
abstract class ScanConvert[INPUT <: Scan](
    scanMeta: ScanMeta[INPUT],
    rule: DataFromReplacementRule)
  extends RapidsConvert[INPUT, Scan, Scan, ScanMeta[INPUT]](scanMeta, rule) {
  }

