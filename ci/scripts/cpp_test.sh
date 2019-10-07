#!/usr/bin/env bash
#
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

source_dir=${1}
build_dir=${2}
install_dir=${3:-${ARROW_HOME}}

export ARROW_TEST_DATA=${source_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/cpp/submodules/parquet-testing/data

pushd ${build_dir}

# TODO(kszucs): revisit
# ARROW-5653 after install target, RPATH is modified on linux. Add this such
# that libraries are found with conda.
# if [[ ! -z "${CONDA_PREFIX}" ]]; then
#   export LD_LIBRARY_PATH=${CONDA_PREFIX}/lib
# fi

ninja unittest

popd
