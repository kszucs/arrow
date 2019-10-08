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

source_dir=${1}/python

export PYARROW_CMAKE_GENERATOR=${CMAKE_GENERATOR:-Ninja}
export PYARROW_BUILD_TYPE=${CMAKE_BUILD_TYPE:-debug}
export PYARROW_WITH_S3=${ARROW_S3:-OFF}
export PYARROW_WITH_ORC=${ARROW_ORC:-OFF}
export PYARROW_WITH_CUDA=${ARROW_CUDA:-OFF}
export PYARROW_WITH_FLIGHT=${ARROW_FLIGHT:-OFF}
export PYARROW_WITH_PLASMA=${ARROW_PLASMA:-OFF}
export PYARROW_WITH_GANDIVA=${ARROW_GANDIVA:-OFF}
export PYARROW_WITH_PARQUET=${ARROW_PARQUET:-OFF}

export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}

pushd ${source_dir}

python setup.py develop

popd
