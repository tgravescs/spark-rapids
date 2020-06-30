/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids.shims;

import java.util.HashMap;
import java.util.Map;

// import com.nvidia.spark.shims.ShimLoaderScala;

/**
 * ShimLoader.
 *
 */
public abstract class ShimLoader {
  public static String SPARK30DATABRICKSSVERSIONNAME = "3.0.0-databricks";
  public static String SPARK30VERSIONNAME = "3.0.0";

  private static SparkShims sparkShims;

  /**
   * The names of the classes for shimming Spark for each major version.
   */
  private static final HashMap<String, String> SPARK_SHIM_CLASSES =
      new HashMap<String, String>();

  static {
    SPARK_SHIM_CLASSES.put(SPARK30VERSIONNAME, "com.nvidia.spark.rapids.shims.Spark300DatabricksShims");
    SPARK_SHIM_CLASSES.put(SPARK30DATABRICKSSVERSIONNAME, "com.nvidia.spark.rapids.shims.Spark300Shims");
  }

  /**
   * Factory method to get an instance of HadoopShims based on the
   * version of Hadoop on the classpath.
   */
  public static synchronized SparkShims getSparkShims() {
    if (sparkShims == null) {
      sparkShims = loadShims(SPARK_SHIM_CLASSES, SparkShims.class);
    }
    return sparkShims;
  }

  private static <T> T loadShims(Map<String, String> classMap, Class<T> xface) {
    String vers = "3.0.0"; // ShimLoaderScala.getVersion();
    String className = classMap.get(vers);
    return createShim(className, xface);
  }

  private static <T> T createShim(String className, Class<T> xface) {
    try {
      Class<?> clazz = Class.forName(className);
      return xface.cast(clazz.newInstance());
    } catch (Exception e) {
      throw new RuntimeException("Could not load shims in class " + className, e);
    }
  }

  /*
  public static String getVersion() {
    String vers = SPARK_VERSION;
    String finalVer = vers;
    // hack for databricks, try to find something more reliable?
    if (SPARK_BUILD_USER.equals("Databricks")) {
        finalVer = vers + "-databricks";
    }
    return finalVer;
  }
  */

  private ShimLoader() {
    // prevent instantiation
  }
}
