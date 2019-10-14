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

ARG org
ARG arch=amd64
ARG conda=latest
ARG python=3.6
FROM ${org}/${arch}-conda-${conda}-python-${python}:latest

ARG pandas=0.25
ARG maven=3.5
ARG node=11
ARG jdk=8

COPY ci/conda_env_sphinx.yml /arrow/ci/
RUN conda install -q \
        --file arrow/ci/conda_env_sphinx.yml \
        pandas=${pandas} \
        maven=${maven} \
        nodejs=${node} \
        openjdk=${jdk} && \
    conda clean --all

RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        gtk-doc-tools \
        libtool \
        libgirepository1.0-dev \
        libglib2.0-doc \
        autoconf-archive \
        gobject-introspection \
        automake && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
