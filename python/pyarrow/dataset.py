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

"""Dataset is currently unstable. APIs subject to change without notice."""

from __future__ import absolute_import

import sys
import functools

if sys.version_info < (3,):
    raise ImportError("Python Dataset bindings require Python 3")

import pyarrow as pa
from pyarrow.fs import FileSelector, FileType, LocalFileSystem
from pyarrow._dataset import (  # noqa
    AndExpression,
    CastExpression,
    CompareOperator,
    ComparisonExpression,
    Dataset,
    DataSource,
    DataSourceDiscovery,
    DefaultPartitionScheme,
    Expression,
    FieldExpression,
    FileFormat,
    FileSystemDataSource,
    FileSystemDataSourceDiscovery,
    FileSystemDiscoveryOptions,
    HivePartitionScheme,
    InExpression,
    IsValidExpression,
    NotExpression,
    OrExpression,
    ParquetFileFormat,
    PartitionScheme,
    PartitionSchemeDiscovery,
    ScalarExpression,
    Scanner,
    ScannerBuilder,
    ScanTask,
    SchemaPartitionScheme,
    TreeDataSource,
)


def partitioning(field_names=None, flavor=None):
    if flavor is None:
        if field_names is None:
            return None  # no partitioning
        elif isinstance(field_names, pa.Schema):
            return SchemaPartitionScheme(field_names)
        elif isinstance(field_names, list):
            return SchemaPartitionScheme.discover(field_names)
        else:
            raise ValueError('Either pass a schema or a list of field names')
    elif flavor == 'hive':
        if isinstance(field_names, pa.Schema):
            return HivePartitionScheme(field_names)
        elif isinstance(field_names, list):
            raise ValueError('Not yet supported')
            # # it would be nice to push down until the C++ implementation
            # schema = HivePartitionScheme.discover()
            # # limit the schema to have only the required fields
            # schema = pa.schema([schema[name] for name in field_names])
            # # create the partitioning
        elif field_names is None:
            return HivePartitionScheme.discover()
        else:
            raise ValueError('Not yet supported')
    else:
        return None


def _ensure_partitioning(obj):
    if isinstance(obj, (PartitionScheme, PartitionSchemeDiscovery)):
        return obj
    elif isinstance(obj, str):
        return partitioning(flavor=obj)
    else:
        return partitioning(obj)


def _ensure_format(obj):
    if isinstance(obj, FileFormat):
        return obj
    elif obj == "parquet":
        return ParquetFileFormat()
    else:
        raise ValueError("format '{0}' is not supported".format(format))


def _ensure_selector(fs, obj):
    if isinstance(obj, str):
        path = fs.get_target_stats([obj])[0]
        if path.type == FileType.Directory:
            # for directory, pass a selector
            return FileSelector(obj, recursive=True)
        else:
            # is a single file path, pass it as a list
            return [obj]
    elif isinstance(obj, list):
        assert all(isinstance(path) for path in obj)
        return obj
    else:
        raise ValueError('Unsupported paths or selector')


def source(src, fs=None, partitioning=None, format=None, **options):
    # src: path/paths/table
    if isinstance(src, pa.Table):
        raise NotImplementedError('InMemorySource is not yet supported')

    if fs is None:
        # TODO handle other file systems
        fs = LocalFileSystem()

    paths = _ensure_selector(fs, src)
    format = _ensure_format(format)
    partitioning = _ensure_partitioning(partitioning)

    options = FileSystemDiscoveryOptions(**options)
    if isinstance(partitioning, PartitionSchemeDiscovery):
        options.partition_scheme_discovery = partitioning
    elif isinstance(partitioning, PartitionScheme):
        options.partition_scheme = partitioning

    return FileSystemDataSourceDiscovery(fs, paths, format, options)


def _unify_schemas(schemas):
    # calculate the subset of fields available in all schemas
    keys_in_order = schemas[0].names
    keys = set(keys_in_order).intersection(*[s.names for s in schemas[1:]])
    if not keys:
        raise ValueError('No common fields found in ...')

    # create subschemas from each individual schema
    schemas = [pa.schema(s.field(k) for k in keys_in_order) for s in schemas]

    # check that the subschemas' fields are equal except their additional
    # key-value metadata
    if any(schemas[0] != s for s in schemas):
        raise ValueError('Schema fields are not equal ...')

    return schemas[0]


def dataset(sources, schema=None):
    # DataSource has no schema, so we cannot check whether it is compatible
    # with the rest of the DataSource objects or DataSourceDiscovery.Inspect()
    # results. So limiting explusively to Discovery objects.
    # TODO(kszucs): support list of filesystem uris
    if isinstance(sources, DataSourceDiscovery):
        return dataset([sources], schema=schema)
    elif isinstance(sources, list):
        assert all(isinstance(obj, DataSourceDiscovery) for obj in sources)
        # trying to create a schema which all data source can coerce to
        schema = _unify_schemas([discovery.inspect() for discovery in sources])
        # finalize the discovery objects to actual data sources
        sources = [discovery.finish() for discovery in sources]
        return Dataset(sources, schema=schema)
    else:
        raise ValueError('wrong usage')
