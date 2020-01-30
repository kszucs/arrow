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

ARG repo
ARG arch=amd64
ARG python=3.6
FROM ${repo}:${arch}-conda-python-${python}

ARG dask=latest
COPY ci/scripts/install_dask.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_dask.sh ${dask}

# The Spark tests currently break with pandas >= 1.0
RUN conda install pandas=0.25.3