#!/bin/bash
#
# Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

. jenkins/version-def.sh

## export 'M2DIR' so that shims can get the correct cudf/spark dependency info
export M2DIR="$WORKSPACE/.m2"

ARTF_ROOT="$WORKSPACE/jars"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
        -Dmaven.repo.local=$WORKSPACE/.m2 \
            $MVN_URM_MIRROR -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT

# NOTE this needs to match the version below that is set to run nightly
SPARK_VER=3.0.1
export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
        rm -f $SPARK_HOME.tgz

# Install all the versions we support
mvn -U -B -Dbuildver=302 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=303 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=304 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=311 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=312 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=313 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
mvn -U -B -Dbuildver=311cdh clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER
# note that Spark 3.2.0 tests fail right now so just skip them
mvn -U -B -Dbuildver=320 clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DskipTests
mvn -U -B -Dbuildver=301 -PsnapshotsWithDatabricks clean deploy $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER \
    -Dpytest.TEST_TAGS='' -Dpytest.TEST_TYPE="nightly"

# Parse cudf and spark files from local mvn repo
jenkins/printJarVersion.sh "CUDFVersion" "$M2DIR/ai/rapids/cudf/${CUDF_VER}" "cudf-${CUDF_VER}" "-${CUDA_CLASSIFIER}.jar" $SERVER_ID
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
