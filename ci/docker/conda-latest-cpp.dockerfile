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

ARG arch=amd64
FROM ${arch}/ubuntu:18.04

ARG arch=amd64
ARG conda=latest
ARG minio=latest
ARG prefix=/opt/conda

# install build essentials
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        ca-certificates \
        ccache \
        g++ \
        gcc \
        git \
        ninja-build \
        pkg-config \
        tzdata \
        wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# arch is unset after the FROM statement, so need to define it again
ENV PATH=${prefix}/bin:$PATH \
    CONDA_PREFIX=${prefix}
# install conda and minio
COPY scripts/install_conda.sh \
     scripts/install_minio.sh \
     /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_conda.sh ${arch} linux ${minio} ${prefix} && \
    /arrow/ci/scripts/install_minio.sh ${arch} linux ${conda} ${prefix}

# install the required conda packages
COPY conda_env_cpp.yml \
     conda_env_gandiva.yml \
     conda_env_unix.yml \
     /arrow/ci/
RUN conda install -q \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_gandiva.yml \
        --file arrow/ci/conda_env_unix.yml && \
    conda clean --all

ENV CC=gcc \
    CXX=g++ \
    ARROW_S3=ON \
    ARROW_GANDIVA=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DEPENDENCY_SOURCE=CONDA \
    ARROW_HOME=$CONDA_PREFIX \
    PARQUET_HOME=$CONDA_PREFIX
