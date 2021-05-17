#!/bin/bash

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

arrow_dir=${1}
build_dir=${2}
# The directory where the final binaries will be stored when scripts finish
dist_dir=${3}

echo "=== Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

echo "=== Building Arrow C++ libraries ==="
export ARROW_TEST_DATA="${arrow_dir}/testing/data"
export PARQUET_TEST_DATA="${arrow_dir}/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

mkdir -p "${build_dir}"
pushd "${build_dir}"

cmake \
  -DARROW_BOOST_USE_SHARED=OFF \
  -DARROW_BROTLI_USE_SHARED=OFF \
  -DARROW_BUILD_TESTS=ON \
  -DARROW_BUILD_UTILITIES=OFF \
  -DARROW_BZ2_USE_SHARED=OFF \
  -DARROW_DATASET=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_GANDIVA_JAVA=ON \
  -DARROW_GANDIVA_STATIC_LIBSTDCPP=ON \
  -DARROW_GANDIVA=ON \
  -DARROW_GFLAGS_USE_SHARED=OFF \
  -DARROW_GRPC_USE_SHARED=OFF \
  -DARROW_JNI=ON \
  -DARROW_LZ4_USE_SHARED=OFF \
  -DARROW_OPENSSL_USE_SHARED=OFF \
  -DARROW_ORC=ON \
  -DARROW_PARQUET=ON \
  -DARROW_PLASMA_JAVA_CLIENT=ON \
  -DARROW_PLASMA=ON \
  -DARROW_PROTOBUF_USE_SHARED=OFF \
  -DARROW_SNAPPY_USE_SHARED=OFF \
  -DARROW_THRIFT_USE_SHARED=OFF \
  -DARROW_UTF8PROC_USE_SHARED=OFF \
  -DARROW_ZSTD_USE_SHARED=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_INSTALL_PREFIX=${build_dir} \
  -DPARQUET_BUILD_EXAMPLES=OFF \
  -DPARQUET_BUILD_EXECUTABLES=OFF \
  -DPARQUET_REQUIRE_ENCRYPTION=OFF \
  ${arrow_dir}/cpp
cmake --build . --target install

if [ "${ARROW_BUILD_TESTS}" == "something" ]; then
  ctest
fi

popd

echo "=== Copying libraries to the distribution folder ==="
mkdir -p "${dist_dir}"
cp -L ${build_dir}/lib/libgandiva_jni.dylib ${dist_dir}
cp -L ${build_dir}/lib/libarrow_dataset_jni.dylib ${dist_dir}
cp -L ${build_dir}/lib/libarrow_orc_jni.dylib ${dist_dir}

echo "=== Checking shared dependencies for libraries ==="

pushd ${dist_dir}
archery linking check-dependencies \
  --allow libarrow_dataset_jni \
  --allow libarrow_orc_jni \
  --allow libc++ \
  --allow libgandiva_jni \
  --allow libncurses \
  --allow libSystem \
  --allow libz \
  libgandiva_jni.dylib \
  libarrow_dataset_jni.dylib \
  libarrow_orc_jni.dylib
popd
