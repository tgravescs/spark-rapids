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

import scala.collection.immutable.HashMap

import com.nvidia.spark.rapids._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.internal.Logging

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}
import org.apache.spark.internal.Logging

object ShimLoader extends Logging {

  val SPARK30DATABRICKSSVERSIONNAME = "3.0.0-databricks"
  val SPARK30VERSIONNAME = "3.0.0"

  private var sparkShims: SparkShims = null
  private var gpuShuffledHashJoinShims: GpuShuffledHashJoinExecBase = null

  /**
   * The names of the classes for shimming Spark for each major version.
   */
  private val SPARK_SHIM_CLASSES = HashMap(
    SPARK30VERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark30Shims",
    SPARK30DATABRICKSSVERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark300DatabricksShims"
  )

  /**
   * Factory method to get an instance of HadoopShims based on the
   * version of Hadoop on the classpath.
   */
  def getSparkShims: SparkShims = {
    if (sparkShims == null) {
      sparkShims = loadShims(SPARK_SHIM_CLASSES, classOf[SparkShims])
    }
    sparkShims
  }

  private val SHUFFLED_HASH_JOIN_SHIM_CLASSES = HashMap(
    SPARK30VERSIONNAME -> "com.nvidia.spark.rapids.shims.GpuShuffledHashJoinExec30",
    SPARK30DATABRICKSSVERSIONNAME -> "com.nvidia.spark.rapids.shims.GpuShuffledHashJoinExec300Databricks"
  )

  def getGpuShuffledHashJoinShims(leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      join: SparkPlan,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan): GpuShuffledHashJoinExecBase = {
    if (sparkShims == null) {
      gpuShuffledHashJoinShims = loadShimsHashJoin(SHUFFLED_HASH_JOIN_SHIM_CLASSES, classOf[GpuShuffledHashJoinExecBase],
        leftKeys, rightKeys, joinType, join, condition, left, right)
    }
    gpuShuffledHashJoinShims 
  }

  private def loadShims[T](classMap: Map[String, String], xface: Class[T]): T = {
    val vers = getVersion();
    val className = classMap.get(vers)
    if (className.isEmpty) {
      throw new Exception(s"No shim layer for $vers")
    } 
    createShim(className.get, xface)
  }

  private def loadShimsHashJoin[T](classMap: Map[String, String], xface: Class[T], leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      join: SparkPlan,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan): T = {
    val vers = getVersion();
    val className = classMap.get(vers)
    if (className.isEmpty) {
      throw new Exception(s"No shim layer for $vers")
    } 
    createShimHashJoin(className.get, xface, leftKeys, rightKeys, joinType, join, condition, left, right)
  }

  private def createShimHashJoin[T](className: String, xface: Class[T], leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      join: SparkPlan,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan): T = try {
    val clazz = Class.forName(className)
    // public static void com.nvidia.spark.rapids.shims.GpuShuffledHashJoinExec30.createInstance(scala.collection.Seq,scala.collection.Seq,org.apache.spark.sql.catalyst.plans.JoinType,org.apache.spark.sql.execution.SparkPlan,scala.Option,org.apache.spark.sql.execution.SparkPlan,org.apache.spark.sql.execution.SparkPlan)
    val resultMethod = clazz.getDeclaredMethod("createInstance", classOf[scala.collection.Seq[org.apache.spark.sql.catalyst.expressions.Expression]],classOf[scala.collection.Seq[org.apache.spark.sql.catalyst.expressions.Expression]],classOf[org.apache.spark.sql.catalyst.plans.JoinType],classOf[org.apache.spark.sql.execution.SparkPlan], classOf[scala.Option[org.apache.spark.sql.catalyst.expressions.Expression]],classOf[org.apache.spark.sql.execution.SparkPlan],classOf[org.apache.spark.sql.execution.SparkPlan])
    val res = resultMethod.invoke(clazz, leftKeys, rightKeys, joinType, join, condition, left, right).asInstanceOf[T]
    // val constructors = clazz.getConstructors()
    // val res = constructors(0).newInstance(leftKeys, rightKeys, joinType, join, condition, left, right).asInstanceOf[T]
    // val res = clazz.newInstance().asInstanceOf[T]
    res
  } catch {
    case e: Exception => throw new RuntimeException("Could not load shims in class " + className, e)
  }


  private def createShim[T](className: String, xface: Class[T]): T = try {
    val clazz = Class.forName(className)
    val res = clazz.newInstance().asInstanceOf[T]
    res
  } catch {
    case e: Exception => throw new RuntimeException("Could not load shims in class " + className, e)
  }

  def getVersion(): String = {
    // hack for databricks, try to find something more reliable?
    if (SPARK_BUILD_USER.equals("Databricks")) {
        SPARK_VERSION + "-databricks"
    } else {
      SPARK_VERSION
    }
  }

}
