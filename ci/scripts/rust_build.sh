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

source_dir=${1}/rust
build_dir=${2:-${source_dir}/target}

mkdir -p ${build_dir}
export CARGO_TARGET_DIR=${build_dir}

# TODO(kszucs): pass target explicitly, like x86_64-pc-windows-msvc?
# TODO(kszucs): run release build?

pushd ${source_dir}

cargo build --all-targets

pushd arrow
# build with no default features
cargo build --all-targets --no-default-features
# build arrow examples
cargo run --example builders --release
cargo run --example dynamic_types --release
cargo run --example read_csv --release
cargo run --example read_csv_infer_schema --release
popd

pushd datafusion
# build datafusion examples
cargo run --example csv_sql --release
cargo run --example parquet_sql --release
popd
