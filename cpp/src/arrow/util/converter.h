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
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"

#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

template <typename T, typename I, typename O>
class ARROW_EXPORT ValueConverter {
 public:
  ValueConverter(const T& type, O options) : type_(type), options_(options){};
  virtual ~ValueConverter() = default;

  template <typename U = T>
  bool IsNull(I);

  template <typename U = T>
  enable_if_has_c_type<U, Result<typename U::c_type>> Convert(I);

  template <typename U = T>
  enable_if_has_string_view<U, Result<util::string_view>> Convert(I);

  // virtual Result<std::shared_ptr<Scalar>> ToScalar(I value);

 protected:
  const T& type_;
  O options_;
};

template <typename I>
class ARROW_EXPORT SequenceConverter {
 public:
  SequenceConverter(std::shared_ptr<ArrayBuilder> builder) : builder_(builder) {}

  virtual ~SequenceConverter() = default;
  std::shared_ptr<ArrayBuilder> builder() { return builder_; }

  virtual Status Append(I value) = 0;
  virtual Status Extend(PyObject* seq, int64_t size) = 0;

  virtual Result<std::shared_ptr<Array>> Finish() = 0;

  // virtual Result<std::shared_ptr<Array>> ToArray(I value);
  // virtual Result<std::shared_ptr<ChunkedArray>> ToChunkedArray(I value);

 protected:
  std::shared_ptr<ArrayBuilder> builder_;
};

template <typename T, typename I, typename VC>
class ARROW_EXPORT TypedSequenceConverter : public SequenceConverter<I> {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

  TypedSequenceConverter(std::shared_ptr<ArrayBuilder> builder, VC value_converter)
      : SequenceConverter<I>(builder),
        value_converter_(std::move(value_converter)),
        typed_builder_(checked_cast<BuilderType*>(builder.get())) {}

  Result<std::shared_ptr<Array>> Finish() {
    std::shared_ptr<Array> out;
    RETURN_NOT_OK(typed_builder_->Finish(&out));
    return out;
  }

 protected:
  VC value_converter_;
  BuilderType* typed_builder_;
};

template <typename T, typename I, typename VC>
class ARROW_EXPORT ListSequenceConverter : public TypedSequenceConverter<T, I, VC> {
 public:
  ListSequenceConverter(std::shared_ptr<ArrayBuilder> builder, VC value_converter,
                        std::shared_ptr<SequenceConverter<I>> child_converter)
      : TypedSequenceConverter<T, I, VC>(builder, value_converter),
        child_converter_(std::move(child_converter)) {}

 protected:
  std::shared_ptr<SequenceConverter<I>> child_converter_;
};

template <typename T, typename I, typename VC>
class ARROW_EXPORT StructSequenceConverter : public TypedSequenceConverter<T, I, VC> {
 public:
  StructSequenceConverter(
      std::shared_ptr<ArrayBuilder> builder, VC value_converter,
      std::vector<std::shared_ptr<SequenceConverter<I>>> child_converters)
      : TypedSequenceConverter<T, I, VC>(builder, value_converter),
        child_converters_(std::move(child_converters)) {}

 protected:
  std::vector<std::shared_ptr<SequenceConverter<I>>> child_converters_;
};

template <typename I, typename O, template <typename> class VC,
          template <typename, typename, typename> class TSC,
          template <typename, typename, typename> class LSC,
          template <typename, typename, typename> class SSC>
Result<std::shared_ptr<SequenceConverter<I>>> MakeSequenceConverter(
    std::shared_ptr<DataType> type, MemoryPool* pool, O options);

// TODO: pass optional listconverter and typed converter classes as template args
template <typename I, typename O, template <typename> class VC,
          template <typename, typename, typename> class TSC,
          template <typename, typename, typename> class LSC,
          template <typename, typename, typename> class SSC>
struct SequenceConverterBuilder {
  Status Visit(const NullType& t) {
    // TODO: merge with the primitive c_type variant below
    using T = NullType;
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using TypedConverter = TSC<T, I, VC<T>>;
    auto builder = std::make_shared<BuilderType>(pool);
    auto value_converter = VC<T>(t, options);
    out->reset(new TypedConverter(std::move(builder), std::move(value_converter)));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<has_c_type<T>::value && !is_interval_type<T>::value, Status> Visit(
      const T& t) {
    // TODO: should be mergable with the string view variant below
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using TypedConverter = TSC<T, I, VC<T>>;
    auto builder = std::make_shared<BuilderType>(type, pool);
    auto value_converter = VC<T>(t, options);
    out->reset(new TypedConverter(std::move(builder), std::move(value_converter)));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<has_string_view<T>::value, Status> Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using TypedConverter = TSC<T, I, VC<T>>;
    auto builder = std::make_shared<BuilderType>(type, pool);
    auto value_converter = VC<T>(t, options);
    out->reset(new TypedConverter(std::move(builder), std::move(value_converter)));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_list_like_type<T>::value && !std::is_same<T, MapType>::value, Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ListConverter = LSC<T, I, VC<T>>;
    ARROW_ASSIGN_OR_RAISE(
        auto child_converter,
        (MakeSequenceConverter<I, O, VC, TSC, LSC, SSC>(t.value_type(), pool, options)));
    auto builder = std::make_shared<BuilderType>(pool, child_converter->builder(), type);
    auto value_converter = VC<T>(t, options);
    out->reset(new ListConverter(std::move(builder), std::move(value_converter),
                                 std::move(child_converter)));
    return Status::OK();
  }

  Status Visit(const StructType& t) {
    using T = StructType;
    using StructConverter = SSC<T, I, VC<T>>;

    std::shared_ptr<SequenceConverter<I>> child_converter;
    std::vector<std::shared_ptr<SequenceConverter<I>>> child_converters;
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;

    for (const auto& field : t.fields()) {
      ARROW_ASSIGN_OR_RAISE(
          child_converter,
          (MakeSequenceConverter<I, O, VC, TSC, LSC, SSC>(field->type(), pool, options)));

      // TODO: use move
      child_converters.emplace_back(child_converter);
      child_builders.emplace_back(child_converter->builder());
    }

    auto builder = std::make_shared<StructBuilder>(type, pool, child_builders);
    auto value_converter = VC<T>(t, options);
    out->reset(new StructConverter(std::move(builder), std::move(value_converter),
                                   std::move(child_converters)));
    return Status::OK();
  }

  Status Visit(const DataType& t) { return Status::NotImplemented(t.name()); }

  const std::shared_ptr<DataType>& type;
  MemoryPool* pool;
  O options;
  std::shared_ptr<SequenceConverter<I>>* out;
};

template <typename I, typename O, template <typename> class VC,
          template <typename, typename, typename> class TSC,
          template <typename, typename, typename> class LSC,
          template <typename, typename, typename> class SSC>
Result<std::shared_ptr<SequenceConverter<I>>> MakeSequenceConverter(
    std::shared_ptr<DataType> type, MemoryPool* pool, O options) {
  std::shared_ptr<SequenceConverter<I>> out;
  SequenceConverterBuilder<I, O, VC, TSC, LSC, SSC> visitor = {type, pool, options, &out};
  RETURN_NOT_OK(VisitTypeInline(*type, &visitor));
  return out;
}

}  // namespace arrow
