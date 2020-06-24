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


cdef class Scalar:
    """
    The base class for scalars.
    """

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly."
                        .format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CScalar]& wrapped):
        self.wrapped = wrapped

    @staticmethod
    cdef wrap(const shared_ptr[CScalar]& wrapped):
        cdef:
            Scalar self
            Type type_id = wrapped.get().type.get().id()

        if type_id == _Type_NA:
            return _NULL

        typ = _scalar_classes[type_id]
        self = typ.__new__(typ)
        self.init(wrapped)

        return self

    cdef inline shared_ptr[CScalar] unwrap(self) nogil:
        return self.wrapped

    @property
    def type(self):
        return pyarrow_wrap_data_type(self.wrapped.get().type)

    def __repr__(self):
        return '<pyarrow.{}: {!r}>'.format(
            self.__class__.__name__, self.as_py()
        )

    def __str__(self):
        return str(self.as_py())

    def __eq__(self, other):
        # TODO(kszucs): use c++ Equals
        if isinstance(other, Scalar):
            other = other.as_py()
        return self.as_py() == other

    def __hash__(self):
        return hash(self.as_py())

    def as_py(self):
        raise NotImplementedError()


_NULL = NA = None


cdef class NullScalar(Scalar):
    """
    Concrete class for null scalars.
    """

    def __cinit__(self):
        global NA
        if NA is not None:
            raise Exception('Cannot create multiple NAType instances')

    def __init__(self):
        pass

    def __repr__(self):
        return 'NULL'

    def __eq__(self, other):
        return NA

    def as_py(self):
        """
        Return this value as a Python None.
        """
        return None


_NULL = NA = NullScalar()


cdef class BooleanScalar(Scalar):
    """
    Concrete class for boolean scalars.
    """

    def as_py(self):
        """
        Return this value as a Python bool.
        """
        cdef CBooleanScalar* sp = <CBooleanScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class UInt8Scalar(Scalar):
    """
    Concrete class for uint8 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CUInt8Scalar* sp = <CUInt8Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class Int8Scalar(Scalar):
    """
    Concrete class for int8 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CInt8Scalar* sp = <CInt8Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class UInt16Scalar(Scalar):
    """
    Concrete class for uint16 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CUInt16Scalar* sp = <CUInt16Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class Int16Scalar(Scalar):
    """
    Concrete class for int16 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CInt16Scalar* sp = <CInt16Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class UInt32Scalar(Scalar):
    """
    Concrete class for uint32 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CUInt32Scalar* sp = <CUInt32Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class Int32Scalar(Scalar):
    """
    Concrete class for int32 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CInt32Scalar* sp = <CInt32Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class UInt64Scalar(Scalar):
    """
    Concrete class for uint64 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CUInt64Scalar* sp = <CUInt64Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class Int64Scalar(Scalar):
    """
    Concrete class for int64 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python int.
        """
        cdef CInt64Scalar* sp = <CInt64Scalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class HalfFloatScalar(Scalar):
    """
    Concrete class for float scalars.
    """

    def as_py(self):
        """
        Return this value as a Python float.
        """
        cdef CHalfFloatScalar* sp = <CHalfFloatScalar*> self.wrapped.get()
        return PyHalf_FromHalf(sp.value) if sp.is_valid else None


cdef class FloatScalar(Scalar):
    """
    Concrete class for float scalars.
    """

    def as_py(self):
        """
        Return this value as a Python float.
        """
        cdef CFloatScalar* sp = <CFloatScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class DoubleScalar(Scalar):
    """
    Concrete class for double scalars.
    """

    def as_py(self):
        """
        Return this value as a Python float.
        """
        cdef CDoubleScalar* sp = <CDoubleScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None


cdef class DecimalScalar(Scalar):
    """
    Concrete class for decimal128 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python Decimal.
        """
        cdef:
            CDecimal128Scalar* sp = <CDecimal128Scalar*> self.wrapped.get()
            CDecimal128Type* dtype = <CDecimal128Type*> sp.type.get()
        return _pydecimal.Decimal(
            frombytes(sp.value.ToString(dtype.scale()))
        )


cdef class Date32Scalar(Scalar):
    """
    Concrete class for date32 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.datetime instance.
        """
        cdef CDate32Scalar* sp = <CDate32Scalar*> self.wrapped.get()

        if sp.is_valid:
            # shift to seconds since epoch
            return (
                datetime.date(1970, 1, 1) + datetime.timedelta(days=sp.value)
            )
        else:
            return None


cdef class Date64Scalar(Scalar):
    """
    Concrete class for date64 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.datetime instance.
        """
        cdef CDate64Scalar* sp = <CDate64Scalar*> self.wrapped.get()

        if sp.is_valid:
            return (
                datetime.date(1970, 1, 1) +
                datetime.timedelta(days=sp.value / 86400000)
            )
        else:
            return None


cdef class Time32Scalar(Scalar):
    """
    Concrete class for time32 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.timedelta instance.
        """
        cdef:
            CTime32Scalar* sp = <CTime32Scalar*> self.wrapped.get()
            CTime32Type* dtype = <CTime32Type*> sp.type.get()

        if sp.is_valid:
            epoch = datetime.datetime(1970, 1, 1)
            if dtype.unit() == TimeUnit_SECOND:
                delta = datetime.timedelta(seconds=sp.value)
            else:
                delta = datetime.timedelta(milliseconds=sp.value)
            return (epoch + delta).time()
        else:
            return None


cdef class Time64Scalar(Scalar):
    """
    Concrete class for time64 scalars.
    """

    def as_py(self):
        """
        Return this value as a Python datetime.timedelta instance.
        """
        cdef:
            CTime64Scalar* sp = <CTime64Scalar*> self.wrapped.get()
            CTime64Type* dtype = <CTime64Type*> sp.type.get()

        if sp.is_valid:
            epoch = datetime.datetime(1970, 1, 1)
            if dtype.unit() == TimeUnit_MICRO:
                delta = datetime.timedelta(microseconds=sp.value)
            else:
                delta = datetime.timedelta(microseconds=sp.value / 1000)
            return (epoch + delta).time()
        else:
            return None


cdef class TimestampScalar(Scalar):
    """
    Concrete class for timestamp scalars.
    """

    @property
    def value(self):
        cdef CTimestampScalar* sp = <CTimestampScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None

    def as_py(self):
        """
        Return this value as a Pandas Timestamp instance (if available),
        otherwise as a Python datetime.timedelta instance.
        """
        cdef:
            CTimestampScalar* sp = <CTimestampScalar*> self.wrapped.get()
            CTimestampType* dtype = <CTimestampType*> sp.type.get()
            TimeUnit unit = dtype.unit()
            int64_t seconds, micros

        if not sp.is_valid:
            return None

        if not dtype.timezone().empty():
            tzinfo = string_to_tzinfo(frombytes(dtype.timezone()))
            if not isinstance(tzinfo, datetime.tzinfo):
                tzinfo = string_to_tzinfo(tzinfo)
        else:
            tzinfo = None

        if unit == TimeUnit_SECOND:
            seconds = sp.value
            micros = 0
        elif unit == TimeUnit_MILLI:
            seconds = sp.value // 1_000
            micros = (sp.value % 1_000) * 1_000
        elif unit == TimeUnit_MICRO:
            seconds = sp.value // 1_000_000
            micros = sp.value % 1_000_000
        else:
            # TimeUnit_NANO: prefer pandas timestamps if available
            if _pandas_api.have_pandas:
                return _pandas_api.pd.Timestamp(sp.value, tz=tzinfo, unit='ns')
            # otherwise safely truncate to microsecond resolution datetime
            if sp.value % 1000 != 0:
                raise ValueError(
                    "Nanosecond timestamp {} is not safely convertible to "
                    "microseconds to convert to datetime.datetime. Install "
                    "pandas to return as Timestamp with nanosecond support or "
                    "access the .value attribute.".format(sp.value)
                )
            seconds = sp.value // 1_000_000_000
            micros = (sp.value // 1_000) % 1_000_000

        # construct datetime object
        dt = datetime.datetime(1970, 1, 1)
        dt += datetime.timedelta(seconds=seconds, microseconds=micros)

        # adjust timezone if set to the datatype
        if tzinfo is not None:
            dt = tzinfo.fromutc(dt)

        return dt


cdef class DurationScalar(Scalar):
    """
    Concrete class for duration scalars.
    """

    @property
    def value(self):
        cdef CDurationScalar* sp = <CDurationScalar*> self.wrapped.get()
        return sp.value if sp.is_valid else None

    def as_py(self):
        """
        Return this value as a Pandas Timestamp instance (if available),
        otherwise as a Python datetime.timedelta instance.
        """
        cdef:
            CDurationScalar* sp = <CDurationScalar*> self.wrapped.get()
            CDurationType* dtype = <CDurationType*> sp.type.get()
            TimeUnit unit = dtype.unit()

        if not sp.is_valid:
            return None

        if unit == TimeUnit_SECOND:
            return datetime.timedelta(seconds=sp.value)
        elif unit == TimeUnit_MILLI:
            return datetime.timedelta(milliseconds=sp.value)
        elif unit == TimeUnit_MICRO:
            return datetime.timedelta(microseconds=sp.value)
        else:
            # TimeUnit_NANO: prefer pandas timestamps if available
            if _pandas_api.have_pandas:
                return _pandas_api.pd.Timedelta(sp.value, unit='ns')
            # otherwise safely truncate to microsecond resolution timedelta
            if sp.value % 1000 != 0:
                raise ValueError(
                    "Nanosecond duration {} is not safely convertible to "
                    "microseconds to convert to datetime.timedelta. Install "
                    "pandas to return as Timedelta with nanosecond support or "
                    "access the .value attribute.".format(sp.value)
                )
            return datetime.timedelta(microseconds=sp.value // 1000)


cdef class BinaryScalar(Scalar):
    """
    Concrete class for binary-like scalars.
    """

    def as_buffer(self):
        """
        Return a view over this value as a Buffer object.
        """
        cdef CBinaryScalar* sp = <CBinaryScalar*> self.wrapped.get()
        return pyarrow_wrap_buffer(sp.value) if sp.is_valid else None

    def as_py(self):
        """
        Return this value as a Python bytes.
        """
        buffer = self.as_buffer()
        if buffer is not None:
            return self.as_buffer().to_pybytes()
        else:
            return None


cdef class LargeBinaryScalar(BinaryScalar):
    pass


cdef class FixedSizeBinaryScalar(BinaryScalar):
    pass


cdef class StringScalar(BinaryScalar):
    """
    Concrete class for string-like (utf8) scalars.
    """

    def as_py(self):
        """
        Return this value as a Python string.
        """
        buffer = self.as_buffer()
        if buffer is not None:
            return frombytes(self.as_buffer().to_pybytes())
        else:
            return None


cdef class LargeStringScalar(StringScalar):
    pass


cdef class ListScalar(Scalar):
    """
    Concrete class for list-like scalars.
    """

    cdef array(self):
        cdef CListScalar* sp = <CListScalar*> self.wrapped.get()
        if sp.is_valid:
            return pyarrow_wrap_array(sp.value)
        else:
            return None

    def __len__(self):
        """
        Return the number of values.
        """
        return len(self.array())

    def __getitem__(self, i):
        """
        Return the value at the given index.
        """
        return self.array()[_normalize_index(i, len(self))]

    def __iter__(self):
        """
        Iterate over this element's values.
        """
        return iter(self.array())

    def as_py(self):
        """
        Return this value as a Python list.
        """
        arr = self.array()
        return None if arr is None else arr.to_pylist()


cdef class FixedSizeListScalar(ListScalar):
    pass


cdef class LargeListScalar(ListScalar):
    pass


cdef class StructScalar(Scalar):
    """
    Concrete class for struct scalars.
    """

    def __len__(self):
        cdef CStructScalar* sp = <CStructScalar*> self.wrapped.get()
        return sp.value.size()

    def __getitem__(self, key):
        """
        Return the child value for the given field.

        Parameters
        ----------
        index : Union[int, str]
            Index / position or name of the field.

        Returns
        -------
        result : Scalar
        """
        cdef:
            CFieldRef ref
            CStructScalar* sp = <CStructScalar*> self.wrapped.get()

        if isinstance(key, (bytes, str)):
            ref = CFieldRef(<c_string> tobytes(key))
        elif isinstance(key, int):
            ref = CFieldRef(<int> key)
        else:
            raise TypeError('Expected integer or string index')

        try:
            return Scalar.wrap(GetResultValue(sp.field(ref)))
        except ArrowInvalid:
            raise IndexError(key)

    def as_py(self):
        """
        Return this value as a Python dict.
        """
        cdef:
            CStructScalar* sp = <CStructScalar*> self.wrapped.get()
            CStructType* dtype = <CStructType*> sp.type.get()
            vector[shared_ptr[CField]] fields = dtype.fields()

        return {frombytes(fields[i].get().name()): Scalar.wrap(sp.value[i])
                for i in range(dtype.num_fields())}


cdef class MapScalar(ListScalar):
    """
    Concrete class for map scalars.
    """

    def __getitem__(self, i):
        """
        Return the value at the given index.
        """
        arr = self.array()
        if arr is None:
            raise IndexError(i)
        dct = arr[_normalize_index(i, len(arr))]
        return (dct['key'], dct['value'])

    def __iter__(self):
        """
        Iterate over this element's values.
        """
        arr = self.array()
        if arr is None:
            return iter(zip(arr.field('key'), arr.field('value')))
        else:
            raise StopIteration

    def as_py(self):
        """
        Return this value as a Python list.
        """
        arr = self.array()
        if arr is not None:
            return list(zip(arr.field('key'), arr.field('value')))
        else:
            return None


cdef class DictionaryScalar(Scalar):
    """
    Concrete class for dictionary-encoded array elements.
    """

    def as_py(self):
        """
        Return this value as a Python object.

        The exact type depends on the dictionary value type.
        """
        cdef:
            CDictionaryScalar* sp = <CDictionaryScalar*> self.wrapped.get()
        return Scalar.wrap(sp.value).as_py()

    @property
    def dictionary_value(self):
        """
        Return this value's underlying dictionary value as a scalar.
        """
        cdef:
            CDictionaryScalar* sp = <CDictionaryScalar*> self.wrapped.get()
        return self.as_py()

    # @property
    # def index_value(self):
    #     """
    #     Return this value's underlying index as a ArrayValue of the right
    #     signed integer type.
    #     """
    #     cdef CDictionaryArray* darr = \
    #         <CDictionaryArray*>(self.sp_array.get())
    #     indices = pyarrow_wrap_array(darr.indices())
    #     return indices[self.index]


# cdef class UnionValue(ArrayValue):
#     """
#     Concrete class for union array elements.
#     """

#     cdef void _set_array(self, const shared_ptr[CArray]& sp_array):
#         self.sp_array = sp_array
#         self.ap = <CUnionArray*> sp_array.get()

#     cdef getitem(self, int64_t i):
#         cdef int child_id = self.ap.child_id(i)
#         cdef shared_ptr[CArray] child = self.ap.field(child_id)
#         cdef CDenseUnionArray* dense
#         if self.ap.mode() == _UnionMode_SPARSE:
#             return box_scalar(self.type[child_id].type, child, i)
#         else:
#             dense = <CDenseUnionArray*> self.ap
#             return box_scalar(self.type[child_id].type, child,
#                               dense.value_offset(i))

#     def as_py(self):
#         """
#         Return this value as a Python object.

#         The exact type depends on the underlying union member.
#         """
#         return self.getitem(self.index).as_py()


#     _Type_DURATION: DurationValue,
#     _Type_SPARSE_UNION: UnionValue,
#     _Type_DENSE_UNION: UnionValue,
# }


cdef dict _scalar_classes = {
    _Type_BOOL: BooleanScalar,
    _Type_UINT8: UInt8Scalar,
    _Type_UINT16: UInt16Scalar,
    _Type_UINT32: UInt32Scalar,
    _Type_UINT64: UInt64Scalar,
    _Type_INT8: Int8Scalar,
    _Type_INT16: Int16Scalar,
    _Type_INT32: Int32Scalar,
    _Type_INT64: Int64Scalar,
    _Type_HALF_FLOAT: HalfFloatScalar,
    _Type_FLOAT: FloatScalar,
    _Type_DOUBLE: DoubleScalar,
    _Type_DECIMAL: DecimalScalar,
    _Type_DATE32: Date32Scalar,
    _Type_DATE64: Date64Scalar,
    _Type_TIME32: Time32Scalar,
    _Type_TIME64: Time64Scalar,
    _Type_TIMESTAMP: TimestampScalar,
    _Type_DURATION: DurationScalar,
    _Type_BINARY: BinaryScalar,
    _Type_LARGE_BINARY: LargeBinaryScalar,
    _Type_FIXED_SIZE_BINARY: FixedSizeBinaryScalar,
    _Type_STRING: StringScalar,
    _Type_LARGE_STRING: LargeStringScalar,
    _Type_LIST: ListScalar,
    _Type_LARGE_LIST: LargeListScalar,
    _Type_FIXED_SIZE_LIST: FixedSizeListScalar,
    _Type_STRUCT: StructScalar,
    _Type_MAP: MapScalar,
    _Type_DICTIONARY: DictionaryScalar,
}


def scalar(value, DataType type=None, bint safe=True,
           MemoryPool memory_pool=None):
    cdef:
        PyConversionOptions options
        shared_ptr[CScalar] scalar
        shared_ptr[CArray] array
        shared_ptr[CChunkedArray] chunked

    options.size = 1
    options.pool = maybe_unbox_memory_pool(memory_pool)
    # options.from_pandas = from_pandas
    if type is not None:
        options.type = type.sp_type

    # with nogil:
    check_status(ConvertPySequence([value], None, options, &chunked))

    assert chunked.get().num_chunks() == 1
    array = chunked.get().chunk(0)
    scalar = GetResultValue(array.get().GetScalar(0))

    return Scalar.wrap(scalar)
