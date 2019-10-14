#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

arrow_dir=${1}
source_dir=${1}/docs
build_dir=${2:-${source_dir}/_build}

export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig:${PKG_CONFIG_PATH}

# Sphinx
# sphinx-build -b html ${source_dir}/source ${build_dir}/html

# C++
pushd ${arrow_dir}/cpp/apidoc
doxygen
# mkdir -p ../../site/asf-site/docs/cpp
# rsync -r html/ ../../site/asf-site/docs/cpp
popd

# C GLib
pushd ${arrow_dir}/c_glib
if [ -f Makefile ]; then
    # Ensure updating to prevent auto re-configure
    touch configure **/Makefile
    make distclean
fi
./autogen.sh
mkdir -p apidocs
pushd apidocs
../configure --prefix=${ARROW_HOME} --enable-gtk-doc
make -j4 GTK_DOC_V_XREF=": "
# mkdir -p ../../site/asf-site/docs/c_glib
# rsync -r doc/arrow-glib/html/ ../../site/asf-site/docs/c_glib/arrow-glib
# rsync -r doc/parquet-glib/html/ ../../site/asf-site/docs/c_glib/parquet-glib
popd
popd

# Make Java documentation
# Override user.home to cache dependencies outside the Docker container
# NB: this assumes that you have arrow-site cloned in the (gitignored) site directory
pushd ${arrow_dir}/java
mvn -Duser.home=`pwd`/.apidocs-m2 -Drat.skip=true -Dcheckstyle.skip=true install site
# mkdir -p ../site/asf-site/docs/java/
# rsync -r target/site/apidocs/ ../site/asf-site/docs/java/
popd

# Make Javascript documentation
pushd ${arrow_dir}/js
npm install
npm run doc
# rsync -r doc/ ../site/asf-site/docs/js
popd
