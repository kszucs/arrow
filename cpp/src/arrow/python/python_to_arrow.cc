// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/python/python_to_arrow.h"
#include "arrow/python/numpy_interop.h"

#include <datetime.h>

#include <algorithm>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/converter.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"

#include "arrow/python/datetime.h"
#include "arrow/python/decimal.h"
#include "arrow/python/helpers.h"
#include "arrow/python/inference.h"
#include "arrow/python/iterators.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/type_traits.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using internal::Chunker;
using internal::Converter;
using internal::DictionaryConverter;
using internal::ListConverter;
using internal::PrimitiveConverter;
using internal::StructConverter;

namespace py {

class PyValue {
 public:
  using I = PyObject*;
  using O = PyConversionOptions;

  static bool IsNull(const O& options, I obj) {
    if (options.from_pandas) {
      return internal::PandasObjectIsNull(obj);
    } else {
      return obj == Py_None;
    }
  }

  static bool IsNaT(const TimestampType*, int64_t value) {
    return internal::npy_traits<NPY_DATETIME>::isnull(value);
  }

  static bool IsNaT(const DurationType*, int64_t value) {
    return internal::npy_traits<NPY_TIMEDELTA>::isnull(value);
  }

  static Result<std::nullptr_t> Convert(const NullType*, const O&, I obj) {
    if (obj == Py_None) {
      return nullptr;
    } else {
      return Status::Invalid("Invalid null value");
    }
  }

  static Result<bool> Convert(const BooleanType*, const O&, I obj) {
    if (obj == Py_True) {
      return true;
    } else if (obj == Py_False) {
      return false;
    } else if (PyArray_IsScalar(obj, Bool)) {
      return reinterpret_cast<PyBoolScalarObject*>(obj)->obval == NPY_TRUE;
    } else {
      return internal::InvalidValue(obj, "tried to convert to boolean");
    }
  }

template <typename Type>
struct ValueConverter<Type, enable_if_integer<Type>> {
  using ValueType = typename Type::c_type;

  template <typename T>
  static enable_if_integer<T, Result<typename T::c_type>> Convert(const T*, const O&,
                                                                  I obj) {
    typename T::c_type value;
    auto status = internal::CIntFromPython(obj, &value);
    if (status.ok()) {
      return value;
    } else if (!internal::PyIntScalar_Check(obj)) {
      return internal::InvalidValue(obj, "tried to convert to int");
    } else {
      return status;
    }
  }

  static Result<uint16_t> Convert(const HalfFloatType*, const O&, I obj) {
    uint16_t value;
    RETURN_NOT_OK(PyFloat_AsHalf(obj, &value));
    return value;
  }

  static Result<float> Convert(const FloatType*, const O&, I obj) {
    float value;
    if (internal::PyFloatScalar_Check(obj)) {
      value = static_cast<float>(PyFloat_AsDouble(obj));
      RETURN_IF_PYERROR();
    } else if (internal::PyIntScalar_Check(obj)) {
      RETURN_NOT_OK(internal::IntegerScalarToFloat32Safe(obj, &value));
    } else {
      return internal::InvalidValue(obj, "tried to convert to float32");
    }
    return value;
  }

  static Result<double> Convert(const DoubleType*, const O&, I obj) {
    double value;
    if (PyFloat_Check(obj)) {
      value = PyFloat_AS_DOUBLE(obj);
    } else if (internal::PyFloatScalar_Check(obj)) {
      // Other kinds of float-y things
      value = PyFloat_AsDouble(obj);
      RETURN_IF_PYERROR();
    } else if (internal::PyIntScalar_Check(obj)) {
      RETURN_NOT_OK(internal::IntegerScalarToDoubleSafe(obj, &value));
    } else {
      return internal::InvalidValue(obj, "tried to convert to double");
    }
    return value;
  }

  static Result<Decimal128> Convert(const Decimal128Type* type, const O&, I obj) {
    Decimal128 value;
    RETURN_NOT_OK(internal::DecimalFromPyObject(obj, *type, &value));
    return value;
  }

  static Result<int32_t> Convert(const Date32Type*, const O&, I obj) {
    int32_t value;
    if (PyDate_Check(obj)) {
      auto pydate = reinterpret_cast<PyDateTime_Date*>(obj);
      value = static_cast<int32_t>(internal::PyDate_to_days(pydate));
    } else {
      RETURN_NOT_OK(
          internal::CIntFromPython(obj, &value, "Integer too large for date32"));
    }
    return value;
  }

  static Result<int64_t> Convert(const Date64Type*, const O&, I obj) {
    int64_t value;
    if (PyDateTime_Check(obj)) {
      auto pydate = reinterpret_cast<PyDateTime_DateTime*>(obj);
      value = internal::PyDateTime_to_ms(pydate);
      // Truncate any intraday milliseconds
      // TODO: introduce an option for this
      value -= value % 86400000LL;
    } else if (PyDate_Check(obj)) {
      auto pydate = reinterpret_cast<PyDateTime_Date*>(obj);
      value = internal::PyDate_to_ms(pydate);
    } else {
      RETURN_NOT_OK(
          internal::CIntFromPython(obj, &value, "Integer too large for date64"));
    }
    return value;
  }

  static Result<int32_t> Convert(const Time32Type* type, const O&, I obj) {
    int32_t value;
    if (PyTime_Check(obj)) {
      // TODO(kszucs): consider to raise if a timezone aware time object is encountered
      switch (type->unit()) {
        case TimeUnit::SECOND:
          value = static_cast<int32_t>(internal::PyTime_to_s(obj));
          break;
        case TimeUnit::MILLI:
          value = static_cast<int32_t>(internal::PyTime_to_ms(obj));
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    } else {
      // TODO(kszucs): validate maximum value?
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value, "Integer too large for int32"));
    }
    return value;
  }

  static Result<int64_t> Convert(const Time64Type* type, const O&, I obj) {
    int64_t value;
    if (PyTime_Check(obj)) {
      // TODO(kszucs): consider to raise if a timezone aware time object is encountered
      switch (type->unit()) {
        case TimeUnit::MICRO:
          value = internal::PyTime_to_us(obj);
          break;
        case TimeUnit::NANO:
          value = internal::PyTime_to_ns(obj);
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    } else {
      // TODO(kszucs): validate maximum value?
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value, "Integer too large for int64"));
    }
    return value;
  }

  static Result<int64_t> Convert(const TimestampType* type, const O& options, I obj) {
    int64_t value;
    if (PyDateTime_Check(obj)) {
      ARROW_ASSIGN_OR_RAISE(int64_t offset, internal::PyDateTime_utcoffset_s(obj));
      if (options.ignore_timezone) {
        offset = 0;
      }
      auto dt = reinterpret_cast<PyDateTime_DateTime*>(obj);
      switch (type->unit()) {
        case TimeUnit::SECOND:
          value = internal::PyDateTime_to_s(dt) - offset;
          break;
        case TimeUnit::MILLI:
          value = internal::PyDateTime_to_ms(dt) - offset * 1000;
          break;
        case TimeUnit::MICRO:
          value = internal::PyDateTime_to_us(dt) - offset * 1000 * 1000;
          break;
        case TimeUnit::NANO:
          // Conversion to nanoseconds can overflow -> check multiply of microseconds
          value = internal::PyDateTime_to_us(dt);
          if (arrow::internal::MultiplyWithOverflow(value, 1000, &value)) {
            return internal::InvalidValue(obj, "out of bounds for nanosecond resolution");
          }
          if (arrow::internal::SubtractWithOverflow(value, offset * 1000 * 1000 * 1000,
                                                    &value)) {
            return internal::InvalidValue(obj, "out of bounds for nanosecond resolution");
          }
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    } else if (PyArray_CheckAnyScalarExact(obj)) {
      // validate that the numpy scalar has np.datetime64 dtype
      std::shared_ptr<DataType> numpy_type;
      RETURN_NOT_OK(NumPyDtypeToArrow(PyArray_DescrFromScalar(obj), &numpy_type));
      if (!numpy_type->Equals(*type)) {
        // TODO(kszucs): the message should highlight the received numpy dtype
        // TODO(kszucs): it also validates the unit, so add the unit to the error message
        return Status::NotImplemented("Expected np.datetime64 but got: ",
                                      numpy_type->ToString());
      }
      return reinterpret_cast<PyDatetimeScalarObject*>(obj)->obval;
    } else {
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    }
    return value;
  }

  static Result<int64_t> Convert(const DurationType* type, const O&, I obj) {
    int64_t value;
    if (PyDelta_Check(obj)) {
      auto dt = reinterpret_cast<PyDateTime_Delta*>(obj);
      switch (type->unit()) {
        case TimeUnit::SECOND:
          value = internal::PyDelta_to_s(dt);
          break;
        case TimeUnit::MILLI:
          value = internal::PyDelta_to_ms(dt);
          break;
        case TimeUnit::MICRO:
          value = internal::PyDelta_to_us(dt);
          break;
        case TimeUnit::NANO:
          value = internal::PyDelta_to_ns(dt);
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    } else if (PyArray_CheckAnyScalarExact(obj)) {
      // validate that the numpy scalar has np.datetime64 dtype
      std::shared_ptr<DataType> numpy_type;
      RETURN_NOT_OK(NumPyDtypeToArrow(PyArray_DescrFromScalar(obj), &numpy_type));
      if (!numpy_type->Equals(*type)) {
        // TODO(kszucs): the message should highlight the received numpy dtype
        // TODO(kszucs): it also validates the unit, so add the unit to the error message
        return Status::NotImplemented("Expected np.timedelta64 but got: ",
                                      numpy_type->ToString());
      }
      return reinterpret_cast<PyTimedeltaScalarObject*>(obj)->obval;
    } else {
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    }
    return value;
  }

  static Result<PyBytesView> Convert(const BaseBinaryType*, const O&, I obj) {
    return PyBytesView::FromString(obj);
  }

  static Result<PyBytesView> Convert(const FixedSizeBinaryType* type, const O&, I obj) {
    ARROW_ASSIGN_OR_RAISE(auto view, PyBytesView::FromString(obj));
    if (ARROW_PREDICT_TRUE(view.size == type->byte_width())) {
      return std::move(view);
    } else {
      std::stringstream ss;
      ss << "expected to be length " << type->byte_width() << " was " << view.size;
      return internal::InvalidValue(obj, ss.str());
    }
  }

  template <typename T>
  static enable_if_string<T, Result<PyBytesView>> Convert(const T*, const O& options,
                                                          I obj) {
    if (options.strict) {
      // Strict conversion, force output to be unicode / utf8 and validate that
      // any binary values are utf8
      ARROW_ASSIGN_OR_RAISE(auto view, PyBytesView::FromString(obj, true));
      // TODO(kszucs): revisit this one
      if (!view.is_utf8) {
        return internal::InvalidValue(obj, "was not a utf8 string");
      }
      return std::move(view);
    } else {
      // Non-strict conversion; keep track of whether values are unicode or bytes
      return PyBytesView::FromString(obj);
    }
  }

  static Result<bool> Convert(const DataType* type, const O&, I obj) {
    return Status::NotImplemented("PyValue::Convert is not implemented for type ", type);
  }
};

template <typename T, typename Enable = void>
class PyPrimitiveConverter;

template <typename T, typename Enable = void>
class PyDictionaryConverter;

template <typename T>
class PyListConverter;

class PyStructConverter;

class PyConverter : public Converter<PyObject*, PyConversionOptions, PyConverter> {
 public:
  template <typename T>
  using PrimitiveConverter = PyPrimitiveConverter<T>;
  template <typename T>
  using DictionaryConverter = PyDictionaryConverter<T>;
  template <typename T>
  using ListConverter = PyListConverter<T>;
  using StructConverter = PyStructConverter;

  Status Extend(PyObject* values, int64_t size) {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->Reserve(size));
    // Iterate over the items adding each one
    return internal::VisitSequence(values, [this](PyObject* item, bool* /* unused */) {
      return this->Append(item);
    });
  }

  Status ExtendMasked(PyObject* values, PyObject* mask, int64_t size) {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->Reserve(size));
    // Iterate over the items adding each one
    return internal::VisitSequenceMasked(
        values, mask, [this](PyObject* item, bool is_masked, bool* /* unused */) {
          if (is_masked) {
            return this->AppendNull();
          } else {
            // This will also apply the null-checking convention in the event
            // that the value is not masked
            return this->Append(item);  // perhaps use AppendValue instead?
          }
        });
  }
};

template <typename T>
class PyPrimitiveConverter<
    T, enable_if_t<is_null_type<T>::value || is_boolean_type<T>::value ||
                   is_number_type<T>::value || is_decimal_type<T>::value ||
                   is_date_type<T>::value || is_time_type<T>::value>>
    : public PrimitiveConverter<T, PyConverter> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto converted, PyValue::Convert(this->primitive_type_, this->options_, value));
      return this->primitive_builder_->Append(converted);
    }
  }
};

template <typename T>
class PyPrimitiveConverter<
    T, enable_if_t<is_timestamp_type<T>::value || is_duration_type<T>::value>>
    : public PrimitiveConverter<T, PyConverter> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto converted, PyValue::Convert(this->primitive_type_, this->options_, value));
      if (PyArray_CheckAnyScalarExact(value) &&
          PyValue::IsNaT(this->primitive_type_, converted)) {
        return this->primitive_builder_->AppendNull();
      } else {
        return this->primitive_builder_->Append(converted);
      }
    }
  }
};

template <typename T>
class PyPrimitiveConverter<T, enable_if_binary<T>>
    : public PrimitiveConverter<T, PyConverter> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto view, PyValue::Convert(this->primitive_type_, this->options_, value));
      return this->primitive_builder_->AppendSafe(
          util::string_view(view.bytes, view.size));
    }
  }
};

template <typename T>
class PyPrimitiveConverter<T, enable_if_string_like<T>>
    : public PrimitiveConverter<T, PyConverter> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto view, PyValue::Convert(this->primitive_type_, this->options_, value));
      if (!view.is_utf8) {
        // observed binary value
        observed_binary_ = true;
      }
      // TODO(kszucs): add CheckOverflow method to binary builders and call it here
      return this->primitive_builder_->AppendSafe(
          util::string_view(view.bytes, view.size));
    }
  }

  Result<std::shared_ptr<Array>> ToArray() override {
    ARROW_ASSIGN_OR_RAISE(auto array, (PrimitiveConverter<T, PyConverter>::ToArray()));
    if (observed_binary_) {
      // If we saw any non-unicode, cast results to BinaryArray
      auto binary_type = TypeTraits<typename T::PhysicalType>::type_singleton();
      return array->View(binary_type);
    } else {
      return array;
    }
  }

 protected:
  bool observed_binary_ = false;
};

template <typename T>
class PyDictionaryConverter<T, enable_if_has_c_type<T>>
    : public DictionaryConverter<T, PyConverter> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->value_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto converted,
                            PyValue::Convert(this->value_type_, this->options_, value));
      // TODO(kszucs): use AppendSafe with checking memory limit BEFORE actual append
      return this->value_builder_->Append(converted);
    }
  }
};

template <typename T>
class PyDictionaryConverter<T, enable_if_has_string_view<T>>
    : public DictionaryConverter<T, PyConverter> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->value_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto view,
                            PyValue::Convert(this->value_type_, this->options_, value));
      // TODO(kszucs): use AppendSafe with checking memory limit BEFORE actual append
      return this->value_builder_->Append(util::string_view(view.bytes, view.size));
    }
  }
};

// If the value type does not match the expected NumPy dtype, then fall through
// to a slower PySequence-based path
#define LIST_FAST_CASE(TYPE_ID, TYPE, NUMPY_TYPE)         \
  case Type::TYPE_ID: {                                   \
    if (PyArray_DESCR(ndarray)->type_num != NUMPY_TYPE) { \
      return this->value_converter_->Extend(value, size); \
    }                                                     \
    return AppendNdarrayTyped<TYPE, NUMPY_TYPE>(ndarray); \
  }

// Use internal::VisitSequence, fast for NPY_OBJECT but slower otherwise
#define LIST_SLOW_CASE(TYPE_ID)                         \
  case Type::TYPE_ID: {                                 \
    return this->value_converter_->Extend(value, size); \
  }

template <typename T>
class PyListConverter : public ListConverter<T, PyConverter> {
 public:
  Status ValidateSize(const BaseListType*, int64_t size) { return Status::OK(); }

  Status ValidateSize(const FixedSizeListType* type, int64_t size) {
    // TODO(kszucs): perhaps this should be handled somewhere else
    if (type->list_size() != size) {
      return Status::Invalid("Length of item not correct: expected ", type->list_size(),
                             " but got array of size ", size);
    } else {
      return Status::OK();
    }
  }

  Status ValidateBuilder(const BaseListType*) { return Status::OK(); }

  Status ValidateBuilder(const MapType*) {
    // TODO(kszucs): perhaps this should be handled somewhere else
    if (this->list_builder_->key_builder()->null_count() > 0) {
      return Status::Invalid("Invalid Map: key field can not contain null values");
    } else {
      return Status::OK();
    }
  }

  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->list_builder_->AppendNull();
    }

    // TODO(kszucs): raise CapacityError if it wouldn't fit in the list builder's
    // memory limit
    RETURN_NOT_OK(this->list_builder_->Append());
    if (PyArray_Check(value)) {
      RETURN_NOT_OK(AppendNdarray(value));
    } else if (PySequence_Check(value)) {
      RETURN_NOT_OK(AppendSequence(value));
    } else {
      return internal::InvalidType(
          value, "was not a sequence or recognized null for conversion to list type");
    }

    return ValidateBuilder(this->list_type_);
  }

  Status AppendSequence(PyObject* value) {
    int64_t size = static_cast<int64_t>(PySequence_Size(value));
    RETURN_NOT_OK(this->ValidateSize(this->list_type_, size));
    return this->value_converter_->Extend(value, size);
  }

  Status AppendNdarray(PyObject* value) {
    PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(value);
    if (PyArray_NDIM(ndarray) != 1) {
      return Status::Invalid("Can only convert 1-dimensional array values");
    }
    const int64_t size = PyArray_SIZE(ndarray);
    RETURN_NOT_OK(this->ValidateSize(this->list_type_, size));

    const auto value_type = this->value_converter_->builder()->type();
    switch (value_type->id()) {
      LIST_SLOW_CASE(NA)
      LIST_FAST_CASE(UINT8, UInt8Type, NPY_UINT8)
      LIST_FAST_CASE(INT8, Int8Type, NPY_INT8)
      LIST_FAST_CASE(UINT16, UInt16Type, NPY_UINT16)
      LIST_FAST_CASE(INT16, Int16Type, NPY_INT16)
      LIST_FAST_CASE(UINT32, UInt32Type, NPY_UINT32)
      LIST_FAST_CASE(INT32, Int32Type, NPY_INT32)
      LIST_FAST_CASE(UINT64, UInt64Type, NPY_UINT64)
      LIST_FAST_CASE(INT64, Int64Type, NPY_INT64)
      LIST_FAST_CASE(HALF_FLOAT, HalfFloatType, NPY_FLOAT16)
      LIST_FAST_CASE(FLOAT, FloatType, NPY_FLOAT)
      LIST_FAST_CASE(DOUBLE, DoubleType, NPY_DOUBLE)
      LIST_FAST_CASE(TIMESTAMP, TimestampType, NPY_DATETIME)
      LIST_FAST_CASE(DURATION, DurationType, NPY_TIMEDELTA)
      LIST_SLOW_CASE(DATE32)
      LIST_SLOW_CASE(DATE64)
      LIST_SLOW_CASE(TIME32)
      LIST_SLOW_CASE(TIME64)
      LIST_SLOW_CASE(BINARY)
      LIST_SLOW_CASE(FIXED_SIZE_BINARY)
      LIST_SLOW_CASE(STRING)
      case Type::LIST: {
        if (PyArray_DESCR(ndarray)->type_num != NPY_OBJECT) {
          return Status::Invalid(
              "Can only convert list types from NumPy object array input");
        }
        return internal::VisitSequence(value, [this](PyObject* item, bool*) {
          return this->value_converter_->Append(item);
        });
      }
      default: {
        return Status::TypeError("Unknown list item type: ", value_type->ToString());
      }
    }
  }

  template <typename ArrowType, int NUMPY_TYPE>
  Status AppendNdarrayTyped(PyArrayObject* ndarray) {
    // no need to go through the conversion
    using NumpyTrait = internal::npy_traits<NUMPY_TYPE>;
    using NumpyType = typename NumpyTrait::value_type;
    using ValueBuilderType = typename TypeTraits<ArrowType>::BuilderType;

    const bool null_sentinels_possible =
        // Always treat Numpy's NaT as null
        NUMPY_TYPE == NPY_DATETIME || NUMPY_TYPE == NPY_TIMEDELTA ||
        // Observing pandas's null sentinels
        (this->options_.from_pandas && NumpyTrait::supports_nulls);

    auto value_builder =
        checked_cast<ValueBuilderType*>(this->value_converter_->builder().get());

    // TODO(wesm): Vector append when not strided
    Ndarray1DIndexer<NumpyType> values(ndarray);
    if (null_sentinels_possible) {
      for (int64_t i = 0; i < values.size(); ++i) {
        if (NumpyTrait::isnull(values[i])) {
          RETURN_NOT_OK(value_builder->AppendNull());
        } else {
          RETURN_NOT_OK(value_builder->Append(values[i]));
        }
      }
    } else {
      for (int64_t i = 0; i < values.size(); ++i) {
        RETURN_NOT_OK(value_builder->Append(values[i]));
      }
    }
    return Status::OK();
  }
};

class PyStructConverter : public StructConverter<PyConverter> {
 public:
  Status Init() override {
    RETURN_NOT_OK(StructConverter<PyConverter>::Init());

    // Store the field names as a PyObjects for dict matching
    num_fields_ = this->struct_type_->num_fields();
    bytes_field_names_.reset(PyList_New(num_fields_));
    unicode_field_names_.reset(PyList_New(num_fields_));
    RETURN_IF_PYERROR();

    for (int i = 0; i < num_fields_; i++) {
      const auto& field_name = this->struct_type_->field(i)->name();
      PyObject* bytes = PyBytes_FromStringAndSize(field_name.c_str(), field_name.size());
      PyObject* unicode =
          PyUnicode_FromStringAndSize(field_name.c_str(), field_name.size());
      RETURN_IF_PYERROR();
      PyList_SET_ITEM(bytes_field_names_.obj(), i, bytes);
      PyList_SET_ITEM(unicode_field_names_.obj(), i, unicode);
    }
    return Status::OK();
  }

  Status InferInputKind(PyObject* value) {
    // Infer input object's type, note that heterogeneous sequences are not allowed
    if (PyDict_Check(value)) {
      input_kind_ = InputKind::DICT;
    } else if (PyTuple_Check(value)) {
      input_kind_ = InputKind::TUPLE;
    } else if (PySequence_Check(value)) {
      input_kind_ = InputKind::ITEMS;
    } else {
      return internal::InvalidType(value,
                                   "was not a dict, tuple, or recognized null value "
                                   "for conversion to struct type");
    }
    return Status::OK();
  }

  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->struct_builder_->AppendNull();
    }
    switch (input_kind_) {
      case InputKind::DICT:
        RETURN_NOT_OK(this->struct_builder_->Append());
        return AppendDict(value);
      case InputKind::TUPLE:
        RETURN_NOT_OK(this->struct_builder_->Append());
        return AppendTuple(value);
      case InputKind::ITEMS:
        RETURN_NOT_OK(this->struct_builder_->Append());
        return AppendItems(value);
      default:
        RETURN_NOT_OK(InferInputKind(value));
        return Append(value);
    }
  }

  Status AppendEmpty() {
    for (int i = 0; i < num_fields_; i++) {
      RETURN_NOT_OK(this->children_[i]->Append(Py_None));
    }
    return Status::OK();
  }

  Status AppendTuple(PyObject* tuple) {
    if (!PyTuple_Check(tuple)) {
      return internal::InvalidType(tuple, "was expecting a tuple");
    }
    if (PyTuple_GET_SIZE(tuple) != num_fields_) {
      return Status::Invalid("Tuple size must be equal to number of struct fields");
    }
    for (int i = 0; i < num_fields_; i++) {
      PyObject* value = PyTuple_GET_ITEM(tuple, i);
      RETURN_NOT_OK(this->children_[i]->Append(value));
    }
    return Status::OK();
  }

  Status InferKeyKind(PyObject* items) {
    // TODO: iterate over the items instead
    for (int i = 0; i < num_fields_; i++) {
      PyObject* tuple = PySequence_GetItem(items, i);
      if (tuple == NULL) {
        RETURN_IF_PYERROR();
      }
      PyObject* key = PyTuple_GET_ITEM(tuple, 0);
      if (key == NULL) {
        RETURN_IF_PYERROR();
      }
      // check equality with unicode field name
      PyObject* name = PyList_GET_ITEM(unicode_field_names_.obj(), i);
      bool are_equal = PyObject_RichCompareBool(key, name, Py_EQ);
      RETURN_IF_PYERROR();
      if (are_equal) {
        key_kind_ = KeyKind::UNICODE;
        return Status::OK();
      }
      // check equality with bytes field name
      name = PyList_GET_ITEM(bytes_field_names_.obj(), i);
      are_equal = PyObject_RichCompareBool(key, name, Py_EQ);
      RETURN_IF_PYERROR();
      if (are_equal) {
        key_kind_ = KeyKind::BYTES;
        return Status::OK();
      }
    }
    return Status::OK();
    // return internal::Invalid(value, "was unable to infer key type");
  }

  Status AppendDict(PyObject* dict) {
    if (!PyDict_Check(dict)) {
      return internal::InvalidType(dict, "was expecting a dict");
    }
    switch (key_kind_) {
      case KeyKind::UNICODE:
        return AppendDict(dict, unicode_field_names_.obj());
      case KeyKind::BYTES:
        return AppendDict(dict, bytes_field_names_.obj());
      default:
        RETURN_NOT_OK(InferKeyKind(PyDict_Items(dict)));
        if (key_kind_ == KeyKind::UNKNOWN) {
          // was unable to infer the type which means that all keys are absent
          return AppendEmpty();
        } else {
          return AppendDict(dict);
        }
    }
  }

  Status AppendDict(PyObject* dict, PyObject* field_names) {
    // NOTE we're ignoring any extraneous dict items
    for (int i = 0; i < num_fields_; i++) {
      PyObject* name = PyList_GET_ITEM(field_names, i);  // borrowed
      PyObject* value = PyDict_GetItem(dict, name);      // borrowed
      if (value == NULL) {
        RETURN_IF_PYERROR();
      }
      RETURN_NOT_OK(this->children_[i]->Append(value ? value : Py_None));
    }
    return Status::OK();
  }

  Status AppendItems(PyObject* items) {
    if (!PySequence_Check(items)) {
      return internal::InvalidType(items, "was expecting a sequence of key-value items");
    }
    // if (PySequence_GET_SIZE(items) != num_fields_) {
    //   return Status::Invalid("Sequence size must be equal to number of struct fields");
    // }
    switch (key_kind_) {
      case KeyKind::UNICODE:
        return AppendItems(items, unicode_field_names_.obj());
      case KeyKind::BYTES:
        return AppendItems(items, bytes_field_names_.obj());
      default:
        RETURN_NOT_OK(InferKeyKind(items));
        if (key_kind_ == KeyKind::UNKNOWN) {
          // was unable to infer the type which means that all keys are absent
          return AppendEmpty();
        } else {
          return AppendItems(items);
        }
    }
  }

  Status AppendItems(PyObject* items, PyObject* field_names) {
    for (int i = 0; i < num_fields_; i++) {
      PyObject* tuple = PySequence_GetItem(items, i);
      if (tuple == NULL) {
        RETURN_IF_PYERROR();
      }
      PyObject* key = PyTuple_GET_ITEM(tuple, 0);
      PyObject* value = PyTuple_GET_ITEM(tuple, 1);
      if (key == NULL || value == NULL) {
        RETURN_IF_PYERROR();
      }
      PyObject* name = PyList_GET_ITEM(field_names, i);
      bool are_equal = PyObject_RichCompareBool(key, name, Py_EQ);
      RETURN_IF_PYERROR();
      if (are_equal) {
        RETURN_NOT_OK(this->children_[i]->Append(value));
      } else {
        return Status::Invalid("Key not equal to the expected field name");
      }
    }
    return Status::OK();
  }

 protected:
  // Whether we're converting from a sequence of dicts or tuples or list of pairs
  enum class InputKind { UNKNOWN, DICT, TUPLE, ITEMS } input_kind_ = InputKind::UNKNOWN;
  // Whether the input dictionary keys' type is python bytes or unicode
  enum class KeyKind { UNKNOWN, BYTES, UNICODE } key_kind_ = KeyKind::UNKNOWN;
  // Store the field names as a PyObjects for dict matching
  OwnedRef bytes_field_names_;
  OwnedRef unicode_field_names_;
  // Store the number of fields for later reuse
  int num_fields_;
};

// Convert *obj* to a sequence if necessary
// Fill *size* to its length.  If >= 0 on entry, *size* is an upper size
// bound that may lead to truncation.
Status ConvertToSequenceAndInferSize(PyObject* obj, PyObject** seq, int64_t* size) {
  if (PySequence_Check(obj)) {
    // obj is already a sequence
    int64_t real_size = static_cast<int64_t>(PySequence_Size(obj));
    if (*size < 0) {
      *size = real_size;
    } else {
      *size = std::min(real_size, *size);
    }
    Py_INCREF(obj);
    *seq = obj;
  } else if (*size < 0) {
    // unknown size, exhaust iterator
    *seq = PySequence_List(obj);
    RETURN_IF_PYERROR();
    *size = static_cast<int64_t>(PyList_GET_SIZE(*seq));
  } else {
    // size is known but iterator could be infinite
    Py_ssize_t i, n = *size;
    PyObject* iter = PyObject_GetIter(obj);
    RETURN_IF_PYERROR();
    OwnedRef iter_ref(iter);
    PyObject* lst = PyList_New(n);
    RETURN_IF_PYERROR();
    for (i = 0; i < n; i++) {
      PyObject* item = PyIter_Next(iter);
      if (!item) break;
      PyList_SET_ITEM(lst, i, item);
    }
    // Shrink list if len(iterator) < size
    if (i < n && PyList_SetSlice(lst, i, n, NULL)) {
      Py_DECREF(lst);
      return Status::UnknownError("failed to resize list");
    }
    *seq = lst;
    *size = std::min<int64_t>(i, *size);
  }
  return Status::OK();
}

Result<std::shared_ptr<ChunkedArray>> ConvertPySequence(PyObject* obj, PyObject* mask,
                                                        const PyConversionOptions& opts) {
  PyAcquireGIL lock;

  PyObject* seq;
  OwnedRef tmp_seq_nanny;
  PyConversionOptions options = opts;  // copy options struct since we modify it below

  std::shared_ptr<DataType> real_type;

  int64_t size = options.size;
  RETURN_NOT_OK(ConvertToSequenceAndInferSize(obj, &seq, &size));
  tmp_seq_nanny.reset(seq);

  // In some cases, type inference may be "loose", like strings. If the user
  // passed pa.string(), then we will error if we encounter any non-UTF8
  // value. If not, then we will allow the result to be a BinaryArray
  auto copied_options = options;
  options.strict = false;

  if (options.type == nullptr) {
    RETURN_NOT_OK(InferArrowType(seq, mask, options.from_pandas, &real_type));
    // TODO(kszucs): remove this
    // if (options.ignore_timezone && real_type->id() == Type::TIMESTAMP) {
    //   const auto& ts_type = checked_cast<const TimestampType&>(*real_type);
    //   real_type = timestamp(ts_type.unit());
    // }
  } else {
    real_type = options.type;
    options.strict = true;
  }
  DCHECK_GE(size, 0);

  ARROW_ASSIGN_OR_RAISE(auto converter,
                        PyConverter::Make(real_type, options.pool, options));
  ARROW_ASSIGN_OR_RAISE(auto chunked_converter, Chunker<PyConverter>::Make(converter));

  // Convert values
  if (mask != nullptr && mask != Py_None) {
    RETURN_NOT_OK(chunked_converter->ExtendMasked(seq, mask, size));
  } else {
    RETURN_NOT_OK(chunked_converter->Extend(seq, size));
  }
  return chunked_converter->ToChunkedArray();
}

}  // namespace py
}  // namespace arrow
