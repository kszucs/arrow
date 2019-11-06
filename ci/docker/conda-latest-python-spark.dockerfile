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

ARG jdk=8
ARG maven=3.5
RUN conda install -q \
        patch \
        pandas \
        openjdk=${jdk} \
        maven=${maven} && \
    conda clean --all

# ENV MAVEN_HOME=/usr/local/maven \
#     M2_HOME=/root/.m2 \
#     PATH=/root/.m2/bin:/usr/local/maven/bin:$PATH

# installing specific version of spark
ARG spark=master
RUN mkdir /spark && wget -q -O - https://github.com/apache/spark/archive/${spark}.tar.gz | tar -xzf - --strip-components=1 -C /spark
# patch spark to build with current Arrow Java
COPY ci/etc/ARROW-6429.patch /tmp/
RUN patch -d /spark -p1 -i /tmp/ARROW-6429.patch && \
    rm /tmp/ARROW-6429.patch

# build cpp with tests
ENV CC=gcc \
    CXX=g++ \
    ARROW_PYTHON=ON \
    ARROW_HDFS=ON \
    ARROW_BUILD_TESTS=OFF
