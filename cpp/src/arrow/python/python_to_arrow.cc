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

namespace py {

// Utility for converting single python objects to their intermediate C representations
// which can be fed to the typed builders
class PyValue {
 public:
  // Type aliases for shorter signature definitions
  using I = PyObject*;
  using O = PyConversionOptions;

  // Used for null checking before actually converting the values
  static bool IsNull(const O& options, I obj) {
    if (options.from_pandas) {
      return internal::PandasObjectIsNull(obj);
    } else {
      return obj == Py_None;
    }
  }

  // Used for post-conversion numpy NaT sentinel checking
  static bool IsNaT(const TimestampType*, int64_t value) {
    return internal::npy_traits<NPY_DATETIME>::isnull(value);
  }

  // Used for post-conversion numpy NaT sentinel checking
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
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value, "Integer too large for int32"));
    }
    return value;
  }

  static Result<int64_t> Convert(const Time64Type* type, const O&, I obj) {
    int64_t value;
    if (PyTime_Check(obj)) {
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
        return Status::NotImplemented("Expected np.timedelta64 but got: ",
                                      numpy_type->ToString());
      }
      return reinterpret_cast<PyTimedeltaScalarObject*>(obj)->obval;
    } else {
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    }
    return value;
  }

  // The binary-like intermediate representation is PyBytesView because it keeps temporary
  // python objects alive (non-contiguous memoryview) and stores whether the original
  // object was unicode encoded or not, which is used for unicode -> bytes coersion if
  // there is a non-unicode object observed.

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

struct MakePyConverterImpl;

class PyConverter {
 public:
  virtual ~PyConverter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status Append(PyObject* value) {
    return Status::NotImplemented("Converter not implemented for type ",
                                  type()->ToString());
  }

  const std::shared_ptr<ArrayBuilder>& builder() const { return builder_; }

  const std::shared_ptr<DataType>& type() const { return type_; }

  PyConversionOptions options() const { return options_; }

  const std::vector<std::shared_ptr<PyConverter>> children() const { return children_; }

  virtual Status Reserve(int64_t additional_capacity) {
    return builder_->Reserve(additional_capacity);
  }

  virtual Status AppendNull() { return builder_->AppendNull(); }

  virtual Result<std::shared_ptr<Array>> ToArray() { return builder_->Finish(); }

  virtual Result<std::shared_ptr<Array>> ToArray(int64_t length) {
    ARROW_ASSIGN_OR_RAISE(auto arr, this->ToArray());
    return arr->Slice(0, length);
  }

  // Convert and append a sequence of values
  Status Extend(PyObject* values, int64_t size) {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->Reserve(size));
    // Iterate over the items adding each one
    return internal::VisitSequence(values, [this](PyObject* item, bool* /* unused */) {
      return this->Append(item);
    });
  }

  // Convert and append a sequence of values masked with a numpy array
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

 protected:
  friend struct MakePyConverterImpl;

  std::shared_ptr<DataType> type_;
  std::shared_ptr<ArrayBuilder> builder_;
  std::vector<std::shared_ptr<PyConverter>> children_;
  PyConversionOptions options_;
};

template <typename T, typename Enable = void>
class PyTypedConverter : public PyConverter {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

  Status Init() override {
    primitive_type_ = checked_cast<const T*>(this->type_.get());
    primitive_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

 protected:
  const T* primitive_type_;
  BuilderType* primitive_builder_;
};

template <typename T, typename Enable = void>
class PyPrimitiveConverter;

template <typename T>
class PyPrimitiveConverter<
    T, enable_if_t<is_null_type<T>::value || is_boolean_type<T>::value ||
                   is_number_type<T>::value || is_decimal_type<T>::value ||
                   is_date_type<T>::value || is_time_type<T>::value>>
    : public PyTypedConverter<T> {
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
    : public PyTypedConverter<T> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto converted, PyValue::Convert(this->primitive_type_, this->options_, value));
      // Numpy NaT sentinels can be checked after the conversion
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
class PyPrimitiveConverter<T, enable_if_binary<T>> : public PyTypedConverter<T> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto view, PyValue::Convert(this->primitive_type_, this->options_, value));
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ValidateOverflow(view.size));
      return this->primitive_builder_->Append(util::string_view(view.bytes, view.size));
    }
  }
};

template <typename T>
class PyPrimitiveConverter<T, enable_if_string_like<T>> : public PyTypedConverter<T> {
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
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ValidateOverflow(view.size));
      return this->primitive_builder_->Append(util::string_view(view.bytes, view.size));
    }
  }

  Result<std::shared_ptr<Array>> ToArray() override {
    ARROW_ASSIGN_OR_RAISE(auto array, (PyTypedConverter<T>::ToArray()));
    if (observed_binary_) {
      // if we saw any non-unicode, cast results to BinaryArray
      auto binary_type = TypeTraits<typename T::PhysicalType>::type_singleton();
      return array->View(binary_type);
    } else {
      return array;
    }
  }

 protected:
  bool observed_binary_ = false;
};

template <typename U>
class PyDictLikeConverter : public PyConverter {
 public:
  using BuilderType = DictionaryBuilder<U>;

  Status Init() override {
    dict_type_ = checked_cast<const DictionaryType*>(this->type_.get());
    value_type_ = checked_cast<const U*>(dict_type_->value_type().get());
    value_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

 protected:
  const DictionaryType* dict_type_;
  const U* value_type_;
  BuilderType* value_builder_;
};

template <typename U, typename Enable = void>
class PyDictionaryConverter;

template <typename U>
class PyDictionaryConverter<U, enable_if_has_c_type<U>> : public PyDictLikeConverter<U> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->value_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto converted,
                            PyValue::Convert(this->value_type_, this->options_, value));
      return this->value_builder_->Append(converted);
    }
  }
};

template <typename U>
class PyDictionaryConverter<U, enable_if_has_string_view<U>>
    : public PyDictLikeConverter<U> {
 public:
  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->value_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto view,
                            PyValue::Convert(this->value_type_, this->options_, value));
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
class PyListConverter : public PyConverter {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

  Status Init() override {
    list_type_ = checked_cast<const T*>(this->type_.get());
    list_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    value_converter_ = this->children_[0];
    return Status::OK();
  }

  Status ValidateOverflow(const MapType*, int64_t size) { return Status::OK(); }

  Status ValidateOverflow(const BaseListType*, int64_t size) {
    return this->list_builder_->ValidateOverflow(size);
  }

  Status ValidateBuilder(const MapType*) {
    if (this->list_builder_->key_builder()->null_count() > 0) {
      return Status::Invalid("Invalid Map: key field can not contain null values");
    } else {
      return Status::OK();
    }
  }

  Status ValidateBuilder(const BaseListType*) { return Status::OK(); }

  Status Append(PyObject* value) override {
    if (PyValue::IsNull(this->options_, value)) {
      return this->list_builder_->AppendNull();
    }

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
    RETURN_NOT_OK(ValidateOverflow(this->list_type_, size));
    return this->value_converter_->Extend(value, size);
  }

  Status AppendNdarray(PyObject* value) {
    PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(value);
    if (PyArray_NDIM(ndarray) != 1) {
      return Status::Invalid("Can only convert 1-dimensional array values");
    }
    const int64_t size = PyArray_SIZE(ndarray);
    RETURN_NOT_OK(ValidateOverflow(this->list_type_, size));

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

 protected:
  const T* list_type_;
  BuilderType* list_builder_;
  std::shared_ptr<PyConverter> value_converter_;
};

class PyStructConverter : public PyConverter {
 public:
  Status Init() override {
    struct_type_ = checked_cast<const StructType*>(this->type_.get());
    struct_builder_ = checked_cast<StructBuilder*>(this->builder_.get());

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

  Status InferKeyKind(PyObject* items) {
    for (int i = 0; i < PySequence_Length(items); i++) {
      // retrieve the key from the passed key-value pairs
      ARROW_ASSIGN_OR_RAISE(auto pair, GetKeyValuePair(items, i));

      // check key exists between the unicode field names
      bool do_contain = PySequence_Contains(unicode_field_names_.obj(), pair.first);
      RETURN_IF_PYERROR();
      if (do_contain) {
        key_kind_ = KeyKind::UNICODE;
        return Status::OK();
      }

      // check key exists between the bytes field names
      do_contain = PySequence_Contains(bytes_field_names_.obj(), pair.first);
      RETURN_IF_PYERROR();
      if (do_contain) {
        key_kind_ = KeyKind::BYTES;
        return Status::OK();
      }
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

  Status AppendItems(PyObject* items) {
    if (!PySequence_Check(items)) {
      return internal::InvalidType(items, "was expecting a sequence of key-value items");
    }
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

  Result<std::pair<PyObject*, PyObject*>> GetKeyValuePair(PyObject* seq, int index) {
    PyObject* pair = PySequence_GetItem(seq, index);
    RETURN_IF_PYERROR();
    if (!PyTuple_Check(pair) || PyTuple_Size(pair) != 2) {
      return internal::InvalidType(pair, "was expecting tuple of (key, value) pair");
    }
    PyObject* key = PyTuple_GetItem(pair, 0);
    RETURN_IF_PYERROR();
    PyObject* value = PyTuple_GetItem(pair, 1);
    RETURN_IF_PYERROR();
    return std::make_pair(key, value);
  }

  Status AppendItems(PyObject* items, PyObject* field_names) {
    auto length = static_cast<int>(PySequence_Size(items));
    RETURN_IF_PYERROR();

    // append the values for the defined fields
    for (int i = 0; i < std::min(num_fields_, length); i++) {
      // retrieve the key-value pair
      ARROW_ASSIGN_OR_RAISE(auto pair, GetKeyValuePair(items, i));

      // validate that the key and the field name are equal
      PyObject* name = PyList_GET_ITEM(field_names, i);
      bool are_equal = PyObject_RichCompareBool(pair.first, name, Py_EQ);
      RETURN_IF_PYERROR();

      // finally append to the respective child builder
      if (are_equal) {
        RETURN_NOT_OK(this->children_[i]->Append(pair.second));
      } else {
        ARROW_ASSIGN_OR_RAISE(auto key_view, PyBytesView::FromString(pair.first));
        ARROW_ASSIGN_OR_RAISE(auto name_view, PyBytesView::FromString(name));
        return Status::Invalid("The expected field name is `", name_view.bytes, "` but `",
                               key_view.bytes, "` was given");
      }
    }
    // insert null values for missing fields
    for (int i = length; i < num_fields_; i++) {
      RETURN_NOT_OK(this->children_[i]->AppendNull());
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

  const StructType* struct_type_;
  StructBuilder* struct_builder_;
};

#define DICTIONARY_CASE(TYPE_ENUM, TYPE_CLASS)                                \
  case Type::TYPE_ENUM:                                                       \
    return Finish<PyDictionaryConverter<TYPE_CLASS>>(std::move(builder), {}); \
    break;

template <typename PyConverter>
class Chunker : public PyConverter {
 public:
  using Self = Chunker<PyConverter>;

  static Result<std::shared_ptr<Self>> Make(std::shared_ptr<PyConverter> converter) {
    auto result = std::make_shared<Self>();
    result->type_ = converter->type();
    result->builder_ = converter->builder();
    result->options_ = converter->options();
    result->children_ = converter->children();
    result->converter_ = std::move(converter);
    return result;
  }

  Status AppendNull() override {
    auto status = converter_->AppendNull();
    if (status.ok()) {
      length_ = this->builder_->length();
    } else if (status.IsCapacityError()) {
      ARROW_RETURN_NOT_OK(FinishChunk());
      return converter_->AppendNull();
    }
    return status;
  }

  Status Append(PyObject* value) override {
    auto status = converter_->Append(value);
    if (status.ok()) {
      length_ = this->builder_->length();
    } else if (status.IsCapacityError()) {
      ARROW_RETURN_NOT_OK(FinishChunk());
      return Append(value);
    }
    return status;
  }

  Status FinishChunk() {
    ARROW_ASSIGN_OR_RAISE(auto chunk, converter_->ToArray(length_));
    this->builder_->Reset();
    length_ = 0;
    chunks_.push_back(chunk);
    return Status::OK();
  }

  Result<std::shared_ptr<ChunkedArray>> ToChunkedArray() {
    ARROW_RETURN_NOT_OK(FinishChunk());
    return std::make_shared<ChunkedArray>(chunks_);
  }

 protected:
  int64_t length_ = 0;
  std::shared_ptr<PyConverter> converter_;
  std::vector<std::shared_ptr<Array>> chunks_;
};
static Result<std::shared_ptr<PyConverter>> MakePyConverter(
    std::shared_ptr<DataType> type, MemoryPool* pool, PyConversionOptions options);
struct MakePyConverterImpl {
  Status Visit(const NullType& t) {
    auto builder = std::make_shared<NullBuilder>(pool);
    return Finish<PyPrimitiveConverter<NullType>>(std::move(builder), {});
  }

  template <typename T>
  enable_if_t<!is_nested_type<T>::value && !is_interval_type<T>::value &&
                  !is_dictionary_type<T>::value && !is_extension_type<T>::value,
              Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ConverterType = PyPrimitiveConverter<T>;

    auto builder = std::make_shared<BuilderType>(type, pool);
    return Finish<ConverterType>(std::move(builder), {});
  }

  template <typename T>
  enable_if_t<is_list_like_type<T>::value && !std::is_same<T, MapType>::value, Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ConverterType = PyListConverter<T>;

    ARROW_ASSIGN_OR_RAISE(auto child_converter,
                          MakePyConverter(t.value_type(), pool, options));
    auto builder = std::make_shared<BuilderType>(pool, child_converter->builder(), type);
    return Finish<ConverterType>(std::move(builder), {std::move(child_converter)});
  }

  Status Visit(const MapType& t) {
    using ConverterType = PyListConverter<MapType>;

    // TODO(kszucs): seems like builders not respect field nullability
    std::vector<std::shared_ptr<Field>> struct_fields{t.key_field(), t.item_field()};
    auto struct_type = std::make_shared<StructType>(struct_fields);
    ARROW_ASSIGN_OR_RAISE(auto struct_converter,
                          MakePyConverter(struct_type, pool, options));

    auto struct_builder = struct_converter->builder();
    auto key_builder = struct_builder->child_builder(0);
    auto item_builder = struct_builder->child_builder(1);
    auto builder = std::make_shared<MapBuilder>(pool, key_builder, item_builder, type);

    return Finish<ConverterType>(std::move(builder), {std::move(struct_converter)});
  }

  Status Visit(const DictionaryType& t) {
    std::unique_ptr<ArrayBuilder> builder;
    ARROW_RETURN_NOT_OK(MakeDictionaryBuilder(pool, type, NULLPTR, &builder));

    switch (t.value_type()->id()) {
      DICTIONARY_CASE(BOOL, BooleanType);
      DICTIONARY_CASE(INT8, Int8Type);
      DICTIONARY_CASE(INT16, Int16Type);
      DICTIONARY_CASE(INT32, Int32Type);
      DICTIONARY_CASE(INT64, Int64Type);
      DICTIONARY_CASE(UINT8, UInt8Type);
      DICTIONARY_CASE(UINT16, UInt16Type);
      DICTIONARY_CASE(UINT32, UInt32Type);
      DICTIONARY_CASE(UINT64, UInt64Type);
      DICTIONARY_CASE(HALF_FLOAT, HalfFloatType);
      DICTIONARY_CASE(FLOAT, FloatType);
      DICTIONARY_CASE(DOUBLE, DoubleType);
      DICTIONARY_CASE(DATE32, Date32Type);
      DICTIONARY_CASE(DATE64, Date64Type);
      DICTIONARY_CASE(BINARY, BinaryType);
      DICTIONARY_CASE(STRING, StringType);
      DICTIONARY_CASE(FIXED_SIZE_BINARY, FixedSizeBinaryType);
      default:
        return Status::NotImplemented("DictionaryArray converter for type ", t.ToString(),
                                      " not implemented");
    }
  }

  Status Visit(const StructType& t) {
    std::shared_ptr<PyConverter> child_converter;
    std::vector<std::shared_ptr<PyConverter>> child_converters;
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;

    for (const auto& field : t.fields()) {
      ARROW_ASSIGN_OR_RAISE(child_converter,
                            MakePyConverter(field->type(), pool, options));

      // TODO: use move
      child_converters.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }

    auto builder = std::make_shared<StructBuilder>(type, pool, child_builders);
    return Finish<PyStructConverter>(std::move(builder), std::move(child_converters));
  }

  Status Visit(const DataType& t) { return Status::NotImplemented(t.name()); }

  template <typename ConverterType>
  Status Finish(std::shared_ptr<ArrayBuilder> builder,
                std::vector<std::shared_ptr<PyConverter>> children) {
    auto converter = new ConverterType();
    converter->type_ = std::move(type);
    converter->builder_ = std::move(builder);
    converter->options_ = options;
    converter->children_ = std::move(children);
    out->reset(converter);
    return Status::OK();
  }

  const std::shared_ptr<DataType> type;
  MemoryPool* pool;
  PyConversionOptions options;
  std::shared_ptr<PyConverter>* out;
};

static Result<std::shared_ptr<PyConverter>> MakePyConverter(
    std::shared_ptr<DataType> type, MemoryPool* pool, PyConversionOptions options) {
  std::shared_ptr<PyConverter> out;
  MakePyConverterImpl visitor = {type, pool, options, &out};
  ARROW_RETURN_NOT_OK(VisitTypeInline(*type, &visitor));
  ARROW_RETURN_NOT_OK(out->Init());
  return out;
}

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
                                                        const PyConversionOptions& opts,
                                                        MemoryPool* pool) {
  PyAcquireGIL lock;

  PyObject* seq;
  OwnedRef tmp_seq_nanny;
  PyConversionOptions options = opts;  // copy options struct since we modify it below

  int64_t size = options.size;
  RETURN_NOT_OK(ConvertToSequenceAndInferSize(obj, &seq, &size));
  tmp_seq_nanny.reset(seq);

  // In some cases, type inference may be "loose", like strings. If the user
  // passed pa.string(), then we will error if we encounter any non-UTF8
  // value. If not, then we will allow the result to be a BinaryArray
  if (options.type == nullptr) {
    ARROW_ASSIGN_OR_RAISE(options.type, InferArrowType(seq, mask, options.from_pandas));
    options.strict = false;
  } else {
    options.strict = true;
  }
  DCHECK_GE(size, 0);

  ARROW_ASSIGN_OR_RAISE(auto converter, MakePyConverter(options.type, pool, options));
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
