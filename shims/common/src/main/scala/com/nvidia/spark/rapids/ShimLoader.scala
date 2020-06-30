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

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}

object ShimLoader {

  val SPARK30DATABRICKSSVERSIONNAME = "3.0.0-databricks"
  val SPARK30VERSIONNAME = "3.0.0"

  private var sparkShims: SparkShims = null

  /**
   * The names of the classes for shimming Spark for each major version.
   */
  private val SPARK_SHIM_CLASSES = HashMap(
    SPARK30VERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark300DatabricksShims",
    SPARK30DATABRICKSSVERSIONNAME -> "com.nvidia.spark.rapids.shims.Spark300Shims"
  )

  /**
   * Factory method to get an instance of HadoopShims based on the
   * version of Hadoop on the classpath.
   */
  def getSparkShims: Unit = {
    if (sparkShims == null) {
      sparkShims = loadShims(SPARK_SHIM_CLASSES, classOf[Nothing])
    }
    sparkShims
  }

  private def loadShims[T](classMap: Map[String, String]) = {
    val vers = getVersion();
    val className = classMap.get(vers)
    createShim(className)
  }

  private def createShim[T](className: String) = try {
    val clazz = Class.forName(className)
    clazz.newInstance.asInstanceOf[SparkShim]
  } catch {
    case e: Nothing =>
      throw new RuntimeException("Could not load shims in class " + className, e)
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
