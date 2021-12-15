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

import java.util

import scala.annotation.tailrec
import scala.collection.mutable

import ai.rapids.cudf
import ai.rapids.cudf.{DType, NvtxColor}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, AttributeSeq, AttributeSet, Expression, ExprId, If, NamedExpression, NullsFirst}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ExplainUtils, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.rapids.{CpuToGpuAggregateBufferConverter, CudfAggregate, GpuAggregateExpression, GpuToCpuAggregateBufferConverter}
import org.apache.spark.sql.rapids.execution.{GpuShuffleMeta, TrampolineUtil}
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object AggregateUtils {

  private val aggs = List("min", "max", "avg", "sum", "count", "first", "last")

  /**
   * Return true if the Attribute passed is one of aggregates in the aggs list.
   * Use it with caution. We are comparing the name of a column looking for anything that matches
   * with the values in aggs.
   */
  def validateAggregate(attributes: AttributeSet): Boolean = {
    attributes.toSeq.exists(attr => aggs.exists(agg => attr.name.contains(agg)))
  }

  /**
   * Return true if there are multiple distinct functions along with non-distinct functions.
   */
  def shouldFallbackMultiDistinct(aggExprs: Seq[AggregateExpression]): Boolean = {
    // Check if there is an `If` within `First`. This is included in the plan for non-distinct
    // functions only when multiple distincts along with non-distinct functions are present in the
    // query. We fall back to CPU in this case when references of `If` are an aggregate. We cannot
    // call `isDistinct` here on aggregateExpressions to get the total number of distinct functions.
    // If there are multiple distincts, the plan is rewritten by `RewriteDistinctAggregates` where
    // regular aggregations and every distinct aggregation is calculated in a separate group.
    aggExprs.map(e => e.aggregateFunction).exists {
      func => {
        func match {
          case First(If(_, _, _), _) if validateAggregate(func.references) => true
          case _ => false
        }
      }
    }
  }

  /**
   * Computes a target input batch size based on the assumption that computation can consume up to
   * 4X the configured batch size.
   * @param confTargetSize user-configured maximum desired batch size
   * @param inputTypes input batch schema
   * @param outputTypes output batch schema
   * @param isReductionOnly true if this is a reduction-only aggregation without grouping
   * @return maximum target batch size to keep computation under the 4X configured batch limit
   */
  def computeTargetBatchSize(
      confTargetSize: Long,
      inputTypes: Seq[DataType],
      outputTypes: Seq[DataType],
      isReductionOnly: Boolean): Long = {
    def typesToSize(types: Seq[DataType]): Long =
      types.map(GpuBatchUtils.estimateGpuMemory(_, nullable = false, rowCount = 1)).sum
    val inputRowSize = typesToSize(inputTypes)
    val outputRowSize = typesToSize(outputTypes)
    // The cudf hash table implementation allocates four 32-bit integers per input row.
    val hashTableRowSize = 4 * 4

    // Using the memory management for joins as a reference, target 4X batch size as a budget.
    var totalBudget = 4 * confTargetSize

    // Compute the amount of memory being consumed per-row in the computation
    var computationBytesPerRow = inputRowSize + hashTableRowSize
    if (isReductionOnly) {
      // Remove the lone output row size from the budget rather than track per-row in computation
      totalBudget -= outputRowSize
    } else {
      // The worst-case memory consumption during a grouping aggregation is the case where the
      // grouping does not combine any input rows, so just as many rows appear in the output.
      computationBytesPerRow += outputRowSize
    }

    // Calculate the max rows that can be processed during computation within the budget
    val maxRows = totalBudget / computationBytesPerRow

    // Finally compute the input target batching size taking into account the cudf row limits
    Math.min(inputRowSize * maxRows, Int.MaxValue)
  }
}

