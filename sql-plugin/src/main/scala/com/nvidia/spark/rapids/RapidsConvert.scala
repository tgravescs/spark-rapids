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

import org.apache.spark.sql.connector.read.Scan

abstract class RapidsConvert[INPUT <: BASE, BASE, OUTPUT <: BASE,
  METATYPE <: RapidsMeta[INPUT, BASE, OUTPUT]](
    rule: DataFromReplacementRule) {

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(meta: METATYPE): OUTPUT

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
abstract class ScanConvert[INPUT <: Scan](
    rule: DataFromReplacementRule)
  extends RapidsConvert[INPUT, Scan, Scan, ScanMeta[INPUT]](rule) {
  }

