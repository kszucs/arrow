#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

source_dir=${1}/java

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

# TODO(kszucs): flag to build with gandiva
if [ "${ARROW_JAVA_SHADE_FLATBUFS}" == "1" ]; then
  SHADE_FLATBUFFERS=-Pshade-flatbuffers
fi

pushd ${source_dir}

mvn -B $MAVEN_OPTS -DskipTests -Drat.skip=true install $SHADE_FLATBUFFERS

popd

###################################
# TODO(kszucs): java jni tests
# # build with gandiva profile
# $TRAVIS_MVN -P arrow-jni -B install -DskipTests -Darrow.cpp.build.dir=$CPP_BUILD_DIR/debug

# # run gandiva tests
# $TRAVIS_MVN test -P arrow-jni -pl gandiva -Darrow.cpp.build.dir=$CPP_BUILD_DIR/debug

##################################
# TODO(kszucs): plasma java client
# PLASMA_JAVA_DIR=${TRAVIS_BUILD_DIR}/java/plasma

# pushd $PLASMA_JAVA_DIR

# $TRAVIS_MVN clean install

# export LD_LIBRARY_PATH=${ARROW_CPP_INSTALL}/lib:$LD_LIBRARY_PATH
# export PLASMA_STORE=${ARROW_CPP_INSTALL}/bin/plasma_store_server

# ldd $PLASMA_STORE

# $TRAVIS_JAVA -cp target/test-classes:target/classes -Djava.library.path=${TRAVIS_BUILD_DIR}/cpp-build/debug/ org.apache.arrow.plasma.PlasmaClientTest
