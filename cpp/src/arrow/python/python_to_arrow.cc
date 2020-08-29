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
#include <iostream>
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

class PyValue {
 public:
  using I = PyObject*;
  using O = PyConversionOptions;

  static bool IsNull(const DataType&, const O& options, I obj) {
    if (options.from_pandas) {
      return internal::PandasObjectIsNull(obj);
    } else {
      return obj == Py_None;
    }
  }

  // static bool IsNull(const TimestampType&, const O&, I obj) {
  //   internal::npy_traits<NPY_DATETIME>::isnull(v);
  // }

  // static bool IsNull(const DurationType&, const O&, I obj) {
  //   internal::npy_traits<NPY_TIMEDELTA>::isnull(v);
  // }

  static Result<std::nullptr_t> Convert(const NullType&, const O&, I obj) {
    if (obj == Py_None) {
      return nullptr;
    } else {
      return Status::Invalid("Invalid null value");
    }
  }

  static Result<bool> Convert(const BooleanType&, const O&, I obj) {
    if (obj == Py_True) {
      return true;
    } else if (obj == Py_False) {
      return false;
    } else if (PyArray_IsScalar(obj, Bool)) {
      return reinterpret_cast<PyBoolScalarObject*>(obj)->obval == NPY_TRUE;
    } else {
      return py::internal::InvalidValue(obj, "tried to convert to boolean");
    }
  }

  template <typename T>
  static enable_if_integer<T, Result<typename T::c_type>> Convert(const T&, const O&,
                                                                  I obj) {
    typename T::c_type value;
    RETURN_NOT_OK(py::internal::CIntFromPython(obj, &value));
    return value;
  }

  static Result<uint16_t> Convert(const HalfFloatType&, const O&, I obj) {
    uint16_t value;
    RETURN_NOT_OK(PyFloat_AsHalf(obj, &value));
    return value;
  }

  static Result<float> Convert(const FloatType&, const O&, I obj) {
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

  static Result<double> Convert(const DoubleType&, const O&, I obj) {
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

  static Result<Decimal128> Convert(const Decimal128Type& type, const O&, I obj) {
    Decimal128 value;
    RETURN_NOT_OK(internal::DecimalFromPyObject(obj, type, &value));
    return value;
  }

  static Result<int32_t> Convert(const Date32Type&, const O&, I obj) {
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

  static Result<int64_t> Convert(const Date64Type&, const O&, I obj) {
    int64_t value;
    if (PyDateTime_Check(obj)) {
      auto pydate = reinterpret_cast<PyDateTime_DateTime*>(obj);
      value = internal::PyDateTime_to_ms(pydate);
      // Truncate any intraday milliseconds
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

  static Result<int32_t> Convert(const Time32Type& type, const O&, I obj) {
    int32_t value;
    if (PyTime_Check(obj)) {
      // TODO(kszucs): consider to raise if a timezone aware time object is encountered
      switch (type.unit()) {
        case TimeUnit::SECOND:
          value = static_cast<int32_t>(internal::PyTime_to_s(obj));
          break;
        case TimeUnit::MILLI:
          value = static_cast<int32_t>(internal::PyTime_to_ms(obj));
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    //     if (PyArray_CheckAnyScalarExact(obj)) {
    //       // convert np.datetime64 / np.timedelta64 depending on Type
    //       ARROW_ASSIGN_OR_RAISE(value, ValueConverter<Type>::FromNumpy(obj, this->unit_));
    //       if (NumpyType<Type>::isnull(value)) {
    //         // checks numpy NaT sentinel after conversion
    //         return this->typed_builder_->AppendNull();
    //       }
    } else {
      // TODO(kszucs): validate maximum value?
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value, "Integer too large for int32"));
    }
    return value;
  }

  static Result<int64_t> Convert(const Time64Type& type, const O&, I obj) {
    int64_t value;
    if (PyTime_Check(obj)) {
      // TODO(kszucs): consider to raise if a timezone aware time object is encountered
      switch (type.unit()) {
        case TimeUnit::MICRO:
          value = internal::PyTime_to_us(obj);
          break;
        case TimeUnit::NANO:
          value = internal::PyTime_to_ns(obj);
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    //     if (PyArray_CheckAnyScalarExact(obj)) {
    //       // convert np.datetime64 / np.timedelta64 depending on Type
    //       ARROW_ASSIGN_OR_RAISE(value, ValueConverter<Type>::FromNumpy(obj, this->unit_));
    //       if (NumpyType<Type>::isnull(value)) {
    //         // checks numpy NaT sentinel after conversion
    //         return this->typed_builder_->AppendNull();
    //       }
    } else {
      // TODO(kszucs): validate maximum value?
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value, "Integer too large for int64"));
    }
    return value;
  }

  static Result<int64_t> Convert(const TimestampType& type, const O& options, I obj) {
    int64_t value;
    if (PyDateTime_Check(obj)) {
      ARROW_ASSIGN_OR_RAISE(int64_t offset, py::internal::PyDateTime_utcoffset_s(obj));
      if (options.ignore_timezone) {
        offset = 0;
      }
      auto dt = reinterpret_cast<PyDateTime_DateTime*>(obj);
      switch (type.unit()) {
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
    // } else if (PyArray_CheckAnyScalarExact(obj)) {
    //   // validate that the numpy scalar has np.datetime64 dtype
    //   std::shared_ptr<DataType> type;
    //   RETURN_NOT_OK(NumPyDtypeToArrow(PyArray_DescrFromScalar(obj), &type));
    //   if (type->id() != TimestampType::type_id) {
    //     // TODO(kszucs): the message should highlight the received numpy dtype
    //     return Status::Invalid("Expected np.datetime64 but got: ", type->ToString());
    //   }
    //   // validate that the time units are matching
    //   if (unit != checked_cast<const TimestampType&>(*type).unit()) {
    //     return Status::NotImplemented(
    //         "Cannot convert NumPy np.datetime64 objects with differing unit");
    //   }
    //   // convert the numpy value
    //   return reinterpret_cast<PyDatetimeScalarObject*>(obj)->obval;
    } else {
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    }
    return value;
  }

  static Result<int64_t> Convert(const DurationType& type, const O&, I obj) {
    int64_t value;
    if (PyDelta_Check(obj)) {
      auto dt = reinterpret_cast<PyDateTime_Delta*>(obj);
      switch (type.unit()) {
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
    } else {
      RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    }
    return value;
  }

  static Result<util::string_view> Convert(const BaseBinaryType&, const O&, I obj) {
    PyBytesView view;
    RETURN_NOT_OK(view.FromString(obj));
    return util::string_view(view.bytes, view.size);
  }

  static Result<util::string_view> Convert(const FixedSizeBinaryType& type, const O&,
                                           I obj) {
    PyBytesView view;
    RETURN_NOT_OK(view.FromString(obj));
    if (ARROW_PREDICT_TRUE(view.size == type.byte_width())) {
      return util::string_view(view.bytes, view.size);
    } else {
      std::stringstream ss;
      ss << "expected to be length " << type.byte_width() << " was " << view.size;
      return internal::InvalidValue(obj, ss.str());
    }
  }

  static Result<bool> Convert(const DataType&, const O&, I obj) {
    return Status::NotImplemented("");
  }

  // template <typename U = T>
  // enable_if_interval<U, Result<int64_t>> Convert(PyObject* obj) {
  //   return Status::NotImplemented("Interval");
  //   // // validate that the numpy scalar has np.timedelta64 dtype
  //   // std::shared_ptr<DataType> type;
  //   // RETURN_NOT_OK(NumPyDtypeToArrow(PyArray_DescrFromScalar(obj), &type));
  //   // if (type->id() != DurationType::type_id) {
  //   //   // TODO(kszucs): the message should highlight the received numpy dtype
  //   //   return Status::Invalid("Expected np.timedelta64 but got: ", type->ToString());
  //   // }
  //   // // validate that the time units are matching
  //   // if (this->type_.unit() != checked_cast<const DurationType&>(*type).unit()) {
  //   //   return Status::NotImplemented(
  //   //       "Cannot convert NumPy np.timedelta64 objects with differing unit");
  //   // }
  //   // // convert the numpy value
  //   // return reinterpret_cast<PyTimedeltaScalarObject*>(obj)->obval;
  // }
};

template <typename T, typename I = PyObject*, typename O = PyConversionOptions>
class PyTypedArrayConverter : public TypedArrayConverter<T, I, O> {
 public:
  using TypedArrayConverter<T, I, O>::TypedArrayConverter;

  // TODO: move it to the parent class
  Status Append(I value) override {
    if (PyValue::IsNull(this->type_, this->options_, value)) {
      return this->builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(auto converted,
                            PyValue::Convert(this->type_, this->options_, value));
      return this->builder_->Append(converted);
    }
  }

  Status Extend(I values, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->builder_->Reserve(size));
    // Iterate over the items adding each one
    return internal::VisitSequence(
        values, [this](I item, bool* /* unused */) { return this->Append(item); });
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

template <typename T, typename I = PyObject*, typename O = PyConversionOptions>
class PyListArrayConverter : public ListArrayConverter<T, I, O> {
 public:
  using ListArrayConverter<T, I, O>::ListArrayConverter;

  // TODO: move it to the parent class and enforce the implementation of Extend
  // or pass an Iterator type which produces size and an iterator of PyObject*
  // objects
  Status Append(I value) override {
    if (PyValue::IsNull(this->type_, this->options_, value)) {
      return this->builder_->AppendNull();
    } else if (PyArray_Check(value)) {
      return AppendNdarray(value);
    } else if (PySequence_Check(value)) {
      return AppendSequence(value);
    } else {
      return internal::InvalidType(
          value, "was not a sequence or recognized null for conversion to list type");
    }
  }

  Status Extend(I values, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->builder_->Reserve(size));
    // Iterate over the items adding each one
    return internal::VisitSequence(
        values, [this](I item, bool* /* unused */) { return this->Append(item); });
  }

  Status AppendSequence(I value) {
    RETURN_NOT_OK(this->builder_->Append());
    int64_t size = static_cast<int64_t>(PySequence_Size(value));
    RETURN_NOT_OK(this->ValidateSize(this->type_, size));
    return this->value_converter_->Extend(value, size);
  }

  Status AppendNdarray(I value) {
    PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(value);
    if (PyArray_NDIM(ndarray) != 1) {
      return Status::Invalid("Can only convert 1-dimensional array values");
    }
    const int64_t size = PyArray_SIZE(ndarray);
    RETURN_NOT_OK(this->ValidateSize(this->type_, size));

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
  Status AppendNdarrayTyped(PyArrayObject* arr) {
    // no need to go through the conversion
    using NumpyTrait = internal::npy_traits<NUMPY_TYPE>;
    using NumpyType = typename NumpyTrait::value_type;
    using ValueBuilderType = typename TypeTraits<ArrowType>::BuilderType;

    RETURN_NOT_OK(this->builder_->Append());

    const bool null_sentinels_possible =
        // Always treat Numpy's NaT as null
        NUMPY_TYPE == NPY_DATETIME || NUMPY_TYPE == NPY_TIMEDELTA ||
        // Observing pandas's null sentinels
        (this->options_.from_pandas && NumpyTrait::supports_nulls);

    auto value_builder =
        checked_cast<ValueBuilderType*>(this->value_converter_->builder().get());

    // TODO(wesm): Vector append when not strided
    Ndarray1DIndexer<NumpyType> values(arr);
    if (null_sentinels_possible) {
      for (int64_t i = 0; i < values.size(); ++i) {
        if (NumpyTrait::isnull(values[i])) {
          RETURN_NOT_OK(value_builder->AppendNull());
        } else {
          RETURN_NOT_OK(value_builder->Append(values[i]));
        }
      }
    } else {
      // TODO(kszucs): use AppendValues() instead?
      for (int64_t i = 0; i < values.size(); ++i) {
        RETURN_NOT_OK(value_builder->Append(values[i]));
      }
    }
    return Status::OK();
  }
};

template <typename T, typename I = PyObject*, typename O = PyConversionOptions>
class PyStructArrayConverter : public StructArrayConverter<T, I, O> {
 public:
  using StructArrayConverter<T, I, O>::StructArrayConverter;

  Status Append(I value) override {
    RETURN_NOT_OK(this->builder_->Append());
    // if (this->value_converter_.IsNull(value)) {
    //   return this->typed_builder_->AppendNull();
    // } else {
    //   int64_t size = static_cast<int64_t>(PySequence_Size(value));
    //   return this->child_converter_->Extend(value, size);
    // }
    return Status::OK();
  }

  Status Extend(I obj, int64_t size) override {
    // /// Ensure we've allocated enough space
    // RETURN_NOT_OK(this->typed_builder_->Reserve(size));
    // // Iterate over the items adding each one
    // return py::internal::VisitSequence(
    //     obj, [this](I item, bool* /* unused */) { return this->Append(item); });
    return Status::OK();
  }
};

using PyArrayConverterBuilder =
    ArrayConverterBuilder<PyObject*, PyConversionOptions, PyTypedArrayConverter,
                          PyListArrayConverter, PyStructArrayConverter>;

////////////////////////////////////////////////////////////////////////

PyObject* create_list() {
  PyObject* listall;
  // NOTE: you don't need to specify the object's type again,
  // since it was already defined above
  listall = Py_BuildValue("[sss]", "pka", "pkb", "ise");
  return listall;
}

Status pinasen() {
  auto options = PyConversionOptions();
  ARROW_ASSIGN_OR_RAISE(auto f,
                        PyArrayConverterBuilder::Make(fixed_size_list(utf8(), 3),
                                                      default_memory_pool(), options));
  auto lst = create_list();
  RETURN_NOT_OK(f->Append(lst));
  RETURN_NOT_OK(f->Append(lst));

  return Status::OK();
}

// ----------------------------------------------------------------------
// ValueConverters
//
// Typed conversion logic for single python objects are encapsulated in
// ValueConverter structs using SFINAE for specialization.
//
// The FromPython medthod is responsible to convert the python object to the
// C++ value counterpart which can be directly appended to the ArrayBuilder or
// Scalar can be constructed from.

// template <typename Type>
// struct ValueConverter<Type, enable_if_string_like<Type>> {
//   static inline Result<PyBytesView> FromPython(PyObject* obj) {
//     // strict conversion, force output to be unicode / utf8 and validate that
//     // any binary values are utf8
//     bool is_utf8 = false;
//     PyBytesView view;

//     RETURN_NOT_OK(view.FromString(obj, &is_utf8));
//     if (!is_utf8) {
//       return internal::InvalidValue(obj, "was not a utf8 string");
//     }
//     return std::move(view);
//   }

//   static inline Result<PyBytesView> FromPython(PyObject* obj, bool* is_utf8) {
//     PyBytesView view;

//     // Non-strict conversion; keep track of whether values are unicode or bytes
//     if (PyUnicode_Check(obj)) {
//       *is_utf8 = true;
//       RETURN_NOT_OK(view.FromUnicode(obj));
//     } else {
//       // If not unicode or bytes, FromBinary will error
//       *is_utf8 = false;
//       RETURN_NOT_OK(view.FromBinary(obj));
//     }
//     return std::move(view);
//   }
// };

//   // Append the contents of a Python sequence to the underlying builder,
//   // virtual version
//   virtual Status ExtendMasked(PyObject* seq, PyObject* mask, int64_t size) = 0;

//  protected:
//   ArrayBuilder* builder_;
//   bool unfinished_builder_;
//   std::vector<std::shared_ptr<Array>> chunks_;
// };

// // ----------------------------------------------------------------------
// // Sequence converters for Binary, FixedSizeBinary, String

// template <typename Type, NullCoding null_coding>
// class BinaryLikeConverter : public TypedConverter<Type, null_coding> {
//  public:
//   using BuilderType = typename TypeTraits<Type>::BuilderType;

//   inline Status AutoChunk(Py_ssize_t size) {
//     // did we reach the builder size limit?
//     if (ARROW_PREDICT_FALSE(this->typed_builder_->value_data_length() + size >
//                             BuilderType::memory_limit())) {
//       // builder would be full, so need to add a new chunk
//       std::shared_ptr<Array> chunk;
//       RETURN_NOT_OK(this->typed_builder_->Finish(&chunk));
//       this->chunks_.emplace_back(std::move(chunk));
//     }
//     return Status::OK();
//   }

//   Status AppendString(const PyBytesView& view) {
//     // check that the value fits in the datatype
//     if (view.size > BuilderType::memory_limit()) {
//       return Status::Invalid("string too large for datatype");
//     }
//     DCHECK_GE(view.size, 0);

//     // create a new chunk if the value would overflow the builder
//     RETURN_NOT_OK(AutoChunk(view.size));

//     // now we can safely append the value to the builder
//     RETURN_NOT_OK(
//         this->typed_builder_->Append(::arrow::util::string_view(view.bytes,
//         view.size)));

//     return Status::OK();
//   }

//  protected:
//   // Create a single instance of PyBytesView here to prevent unnecessary object
//   // creation/destruction
//   PyBytesView string_view_;
// };

// // For String/UTF8, if strict_conversions enabled, we reject any non-UTF8,
// // otherwise we allow but return results as BinaryArray
// template <typename Type, bool Strict, NullCoding null_coding>
// class StringConverter : public BinaryLikeConverter<Type, null_coding> {
//  public:
//   StringConverter() : binary_count_(0) {}

//   Status AppendValue(PyObject* obj) override {
//     if (Strict) {
//       // raise if the object is not unicode or not an utf-8 encoded bytes
//       ARROW_ASSIGN_OR_RAISE(this->string_view_, ValueConverter<Type>::FromPython(obj));
//     } else {
//       // keep track of whether values are unicode or bytes; if any bytes are
//       // observe, the result will be bytes
//       bool is_utf8;
//       ARROW_ASSIGN_OR_RAISE(this->string_view_,
//                             ValueConverter<Type>::FromPython(obj, &is_utf8));
//       if (!is_utf8) {
//         ++binary_count_;
//       }
//     }
//     return this->AppendString(this->string_view_);
//   }

//   Status GetResult(std::shared_ptr<ChunkedArray>* out) override {
//     RETURN_NOT_OK(SeqConverter::GetResult(out));

//     // If we saw any non-unicode, cast results to BinaryArray
//     if (binary_count_) {
//       // We should have bailed out earlier
//       DCHECK(!Strict);
//       auto binary_type = TypeTraits<typename Type::PhysicalType>::type_singleton();
//       return (*out)->View(binary_type).Value(out);
//     }
//     return Status::OK();
//   }

//  protected:
//   int64_t binary_count_;
// };

// // ----------------------------------------------------------------------
// // Convert maps

// // Define a MapConverter as a ListConverter that uses MapBuilder.value_builder
// // to append struct of key/value pairs
// template <NullCoding null_coding>
// class MapConverter : public BaseListConverter<MapType, null_coding> {
//  public:
//   using BASE = BaseListConverter<MapType, null_coding>;

//   explicit MapConverter(bool from_pandas, bool strict_conversions, bool
//   ignore_timezone)
//       : BASE(from_pandas, strict_conversions, ignore_timezone), key_builder_(nullptr)
//       {}

//   Status Append(PyObject* obj) override {
//     RETURN_NOT_OK(BASE::Append(obj));
//     return VerifyLastStructAppended();
//   }

//   Status Extend(PyObject* seq, int64_t size) override {
//     RETURN_NOT_OK(BASE::Extend(seq, size));
//     return VerifyLastStructAppended();
//   }

//   Status ExtendMasked(PyObject* seq, PyObject* mask, int64_t size) override {
//     RETURN_NOT_OK(BASE::ExtendMasked(seq, mask, size));
//     return VerifyLastStructAppended();
//   }

//  protected:
//   Status VerifyLastStructAppended() {
//     // The struct_builder may not have field_builders initialized in constructor, so
//     // assign key_builder lazily
//     if (key_builder_ == nullptr) {
//       auto struct_builder =
//           checked_cast<StructBuilder*>(BASE::value_converter_->builder());
//       key_builder_ = struct_builder->field_builder(0);
//     }
//     if (key_builder_->null_count() > 0) {
//       return Status::Invalid("Invalid Map: key field can not contain null values");
//     }
//     return Status::OK();
//   }

//  private:
//   ArrayBuilder* key_builder_;
// };

// // ----------------------------------------------------------------------
// // Convert structs

// template <NullCoding null_coding>
// class StructConverter : public TypedConverter<StructType, null_coding> {
//  public:
//   explicit StructConverter(bool from_pandas, bool strict_conversions,
//                            bool ignore_timezone)
//       : from_pandas_(from_pandas),
//         strict_conversions_(strict_conversions),
//         ignore_timezone_(ignore_timezone) {}

//   Status Init(ArrayBuilder* builder) override {
//     this->builder_ = builder;
//     this->typed_builder_ = checked_cast<StructBuilder*>(builder);
//     auto struct_type = checked_pointer_cast<StructType>(builder->type());

//     num_fields_ = this->typed_builder_->num_fields();
//     DCHECK_EQ(num_fields_, struct_type->num_fields());

//     field_name_bytes_list_.reset(PyList_New(num_fields_));
//     field_name_unicode_list_.reset(PyList_New(num_fields_));
//     RETURN_IF_PYERROR();

//     // Initialize the child converters and field names
//     for (int i = 0; i < num_fields_; i++) {
//       const std::string& field_name(struct_type->field(i)->name());
//       std::shared_ptr<DataType> field_type(struct_type->field(i)->type());

//       std::unique_ptr<SeqConverter> value_converter;
//       RETURN_NOT_OK(GetConverter(field_type, from_pandas_, strict_conversions_,
//                                  ignore_timezone_, &value_converter));
//       RETURN_NOT_OK(value_converter->Init(this->typed_builder_->field_builder(i)));
//       value_converters_.push_back(std::move(value_converter));

//       // Store the field name as a PyObject, for dict matching
//       PyObject* bytesobj =
//           PyBytes_FromStringAndSize(field_name.c_str(), field_name.size());
//       PyObject* unicodeobj =
//           PyUnicode_FromStringAndSize(field_name.c_str(), field_name.size());
//       RETURN_IF_PYERROR();
//       PyList_SET_ITEM(field_name_bytes_list_.obj(), i, bytesobj);
//       PyList_SET_ITEM(field_name_unicode_list_.obj(), i, unicodeobj);
//     }

//     return Status::OK();
//   }

//   Status AppendValue(PyObject* obj) override {
//     RETURN_NOT_OK(this->typed_builder_->Append());
//     // Note heterogeneous sequences are not allowed
//     if (ARROW_PREDICT_FALSE(source_kind_ == SourceKind::UNKNOWN)) {
//       if (PyDict_Check(obj)) {
//         source_kind_ = SourceKind::DICTS;
//       } else if (PyTuple_Check(obj)) {
//         source_kind_ = SourceKind::TUPLES;
//       }
//     }
//     if (PyDict_Check(obj) && source_kind_ == SourceKind::DICTS) {
//       return AppendDictItem(obj);
//     } else if (PyTuple_Check(obj) && source_kind_ == SourceKind::TUPLES) {
//       return AppendTupleItem(obj);
//     } else {
//       return internal::InvalidType(obj,
//                                    "was not a dict, tuple, or recognized null value"
//                                    " for conversion to struct type");
//     }
//   }

//   // Append a missing item
//   Status AppendNull() override { return this->typed_builder_->AppendNull(); }

//  protected:
//   Status AppendDictItem(PyObject* obj) {
//     if (dict_key_kind_ == DictKeyKind::UNICODE) {
//       return AppendDictItemWithUnicodeKeys(obj);
//     }
//     if (dict_key_kind_ == DictKeyKind::BYTES) {
//       return AppendDictItemWithBytesKeys(obj);
//     }
//     for (int i = 0; i < num_fields_; i++) {
//       PyObject* nameobj = PyList_GET_ITEM(field_name_unicode_list_.obj(), i);
//       PyObject* valueobj = PyDict_GetItem(obj, nameobj);
//       if (valueobj != NULL) {
//         dict_key_kind_ = DictKeyKind::UNICODE;
//         return AppendDictItemWithUnicodeKeys(obj);
//       }
//       RETURN_IF_PYERROR();
//       // Unicode key not present, perhaps bytes key is?
//       nameobj = PyList_GET_ITEM(field_name_bytes_list_.obj(), i);
//       valueobj = PyDict_GetItem(obj, nameobj);
//       if (valueobj != NULL) {
//         dict_key_kind_ = DictKeyKind::BYTES;
//         return AppendDictItemWithBytesKeys(obj);
//       }
//       RETURN_IF_PYERROR();
//     }
//     // If we come here, it means all keys are absent
//     for (int i = 0; i < num_fields_; i++) {
//       RETURN_NOT_OK(value_converters_[i]->Append(Py_None));
//     }
//     return Status::OK();
//   }

//   Status AppendDictItemWithBytesKeys(PyObject* obj) {
//     return AppendDictItem(obj, field_name_bytes_list_.obj());
//   }

//   Status AppendDictItemWithUnicodeKeys(PyObject* obj) {
//     return AppendDictItem(obj, field_name_unicode_list_.obj());
//   }

//   Status AppendDictItem(PyObject* obj, PyObject* field_name_list) {
//     // NOTE we're ignoring any extraneous dict items
//     for (int i = 0; i < num_fields_; i++) {
//       PyObject* nameobj = PyList_GET_ITEM(field_name_list, i);  // borrowed
//       PyObject* valueobj = PyDict_GetItem(obj, nameobj);        // borrowed
//       if (valueobj == NULL) {
//         RETURN_IF_PYERROR();
//       }
//       RETURN_NOT_OK(value_converters_[i]->Append(valueobj ? valueobj : Py_None));
//     }
//     return Status::OK();
//   }

//   Status AppendTupleItem(PyObject* obj) {
//     if (PyTuple_GET_SIZE(obj) != num_fields_) {
//       return Status::Invalid("Tuple size must be equal to number of struct fields");
//     }
//     for (int i = 0; i < num_fields_; i++) {
//       PyObject* valueobj = PyTuple_GET_ITEM(obj, i);
//       RETURN_NOT_OK(value_converters_[i]->Append(valueobj));
//     }
//     return Status::OK();
//   }

//   std::vector<std::unique_ptr<SeqConverter>> value_converters_;
//   OwnedRef field_name_unicode_list_;
//   OwnedRef field_name_bytes_list_;
//   int num_fields_;
//   // Whether we're converting from a sequence of dicts or tuples
//   enum class SourceKind { UNKNOWN, DICTS, TUPLES } source_kind_ = SourceKind::UNKNOWN;
//   enum class DictKeyKind {
//     UNKNOWN,
//     BYTES,
//     UNICODE
//   } dict_key_kind_ = DictKeyKind::UNKNOWN;
//   bool from_pandas_;
//   bool strict_conversions_;
//   bool ignore_timezone_;
// };

// #define PRIMITIVE(TYPE_ENUM, TYPE)                                                   \
//   case Type::TYPE_ENUM:                                                              \
//     *out = std::unique_ptr<SeqConverter>(new PrimitiveConverter<TYPE, null_coding>); \
//     break;

// #define SIMPLE_CONVERTER_CASE(TYPE_ENUM, TYPE_CLASS)                   \
//   case Type::TYPE_ENUM:                                                \
//     *out = std::unique_ptr<SeqConverter>(new TYPE_CLASS<null_coding>); \
//     break;

// // ----------------------------------------------------------------------

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

Status ConvertPySequence(PyObject* sequence_source, PyObject* mask,
                         const PyConversionOptions& options,
                         std::shared_ptr<ChunkedArray>* out) {
  PyAcquireGIL lock;

  PyObject* seq;
  OwnedRef tmp_seq_nanny;

  std::shared_ptr<DataType> real_type;

  int64_t size = options.size;
  RETURN_NOT_OK(ConvertToSequenceAndInferSize(sequence_source, &seq, &size));
  tmp_seq_nanny.reset(seq);

  // In some cases, type inference may be "loose", like strings. If the user
  // passed pa.string(), then we will error if we encounter any non-UTF8
  // value. If not, then we will allow the result to be a BinaryArray
  bool strict_conversions = false;

  if (options.type == nullptr) {
    RETURN_NOT_OK(InferArrowType(seq, mask, options.from_pandas, &real_type));
    // TODO(kszucs): remove this
    // if (options.ignore_timezone && real_type->id() == Type::TIMESTAMP) {
    //   const auto& ts_type = checked_cast<const TimestampType&>(*real_type);
    //   real_type = timestamp(ts_type.unit());
    // }
  } else {
    real_type = options.type;
    strict_conversions = true;
  }
  DCHECK_GE(size, 0);

  ARROW_ASSIGN_OR_RAISE(auto converter,
                        PyArrayConverterBuilder::Make(real_type, options.pool, options));
  // // Create the sequence converter, initialize with the builder
  // std::unique_ptr<SeqConverter> converter;
  // RETURN_NOT_OK(GetConverter(real_type, options.from_pandas, strict_conversions,
  //                            options.ignore_timezone, &converter));

  // // Create ArrayBuilder for type, then pass into the SeqConverter
  // // instance. The reason this is created here rather than in GetConverter is
  // // because of nested types (child SeqConverter objects need the child
  // // builders created by MakeBuilder)
  // std::unique_ptr<ArrayBuilder> type_builder;
  // RETURN_NOT_OK(MakeBuilder(options.pool, real_type, &type_builder));
  // RETURN_NOT_OK(converter->Init(type_builder.get()));

  // Convert values
  // if (mask != nullptr && mask != Py_None) {
  //   RETURN_NOT_OK(converter->ExtendMasked(seq, mask, size));
  // } else {
  //   RETURN_NOT_OK(converter->Extend(seq, size));
  // }
  RETURN_NOT_OK(converter->Extend(seq, size));

  // Retrieve result. Conversion may yield one or more array values
  // return converter->GetResult(out);
  ARROW_ASSIGN_OR_RAISE(auto result, converter->Finish());
  ArrayVector chunks{result};
  *out = std::make_shared<ChunkedArray>(chunks, real_type);
  return Status::OK();
}

Status ConvertPySequence(PyObject* obj, const PyConversionOptions& options,
                         std::shared_ptr<ChunkedArray>* out) {
  return ConvertPySequence(obj, nullptr, options, out);
}

}  // namespace py
}  // namespace arrow
