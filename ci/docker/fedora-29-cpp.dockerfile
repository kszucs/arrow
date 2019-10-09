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

ARG arch
FROM ${arch}/fedora:29

# install dependencies
RUN dnf update -y && \
	dnf install -y \
        autoconf \
        bison \
        boost-devel \
        brotli-devel \
        bzip2-devel \
        ccache \
        clang-devel \
        cmake \
        double-conversion-devel \
        flatbuffers-devel \
        flex \
        java-openjdk-devel \
        java-openjdk-headless \
        gcc \
        gcc-c++ \
        glog-devel \
        gflags-devel \
        gtest-devel \
        gmock-devel \
        google-benchmark-devel \
        git \
        libzstd-devel \
        llvm-devel \
        llvm-static \
        lz4-devel \
        make \
        ninja-build \
        openssl-devel \
        python \
        rapidjson-devel \
        re2-devel \
        snappy-devel \
        thrift-devel \
        zlib-devel

# * c-ares cmake config is not installed on Fedora but gRPC needs it
#   when built via ExternalProject: https://bugzilla.redhat.com/show_bug.cgi?id=1687844
# * protobuf libraries in Fedora 29 are too old for gRPC
ENV CMAKE_ARGS="\
-Dc-ares_SOURCE=BUNDLED \
-DgRPC_SOURCE=BUNDLED \
-DORC_SOURCE=BUNDLED \
-DProtobuf_SOURCE=BUNDLED"

ENV CC=gcc \
    CXX=g++ \
    ARROW_ORC=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_ZSTD=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_PARQUET=ON \
    ARROW_FLIGHT=ON \
    ARROW_GANDIVA=OFF \
    ARROW_GANDIVA_JAVA=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_HOME=/usr/local

# Gandiva test is failing with:
# Running gandiva-internals-test, redirecting output into /build/cpp/build/test-logs/gandiva-internals-test.txt (attempt 1/1)
# 1364
# : CommandLine Error: Option 'x86-experimental-vector-widening-legalization' registered more than once!
# 1365
# LLVM ERROR: inconsistency in registered CommandLine options
# 1366
# /build/cpp/src/gandiva
