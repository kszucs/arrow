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

template <typename I, typename O>
class ARROW_EXPORT ArrayConverter {
 public:
  ArrayConverter(std::shared_ptr<ArrayBuilder> builder, O options)
      : builder_(builder), options_(options) {}

  virtual ~ArrayConverter() = default;
  std::shared_ptr<ArrayBuilder> builder() { return builder_; }
  O options() { return options_; }

  virtual Status Append(I value) = 0;
  virtual Status Extend(I seq, int64_t size) = 0;

  virtual Result<std::shared_ptr<Array>> Finish() = 0;

  // virtual Result<std::shared_ptr<Array>> ToArray(I value);
  // virtual Result<std::shared_ptr<ChunkedArray>> ToChunkedArray(I value);

 protected:
  std::shared_ptr<ArrayBuilder> builder_;
  O options_;
};

template <typename T, typename I, typename O>
class ARROW_EXPORT TypedArrayConverter : public ArrayConverter<I, O> {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

  TypedArrayConverter(std::shared_ptr<ArrayBuilder> builder, O options)
      : ArrayConverter<I, O>(builder, options),
        type_(checked_cast<const T&>(*builder->type())),
        typed_builder_(checked_cast<BuilderType*>(builder.get())) {}

  Result<std::shared_ptr<Array>> Finish() {
    std::shared_ptr<Array> out;
    RETURN_NOT_OK(typed_builder_->Finish(&out));
    return out;
  }

 protected:
  const T& type_;
  BuilderType* typed_builder_;
};

template <typename T, typename I, typename O>
class ARROW_EXPORT ListArrayConverter : public TypedArrayConverter<T, I, O> {
 public:
  ListArrayConverter(std::shared_ptr<ArrayBuilder> builder,
                     std::shared_ptr<ArrayConverter<I, O>> child_converter, O options)
      : TypedArrayConverter<T, I, O>(builder, options),
        child_converter_(std::move(child_converter)) {}

 protected:
  std::shared_ptr<ArrayConverter<I, O>> child_converter_;
};

template <typename T, typename I, typename O>
class ARROW_EXPORT StructArrayConverter : public TypedArrayConverter<T, I, O> {
 public:
  StructArrayConverter(
      std::shared_ptr<ArrayBuilder> builder,
      std::vector<std::shared_ptr<ArrayConverter<I, O>>> child_converters, O options)
      : TypedArrayConverter<T, I, O>(builder, options),
        child_converters_(std::move(child_converters)) {}

 protected:
  std::vector<std::shared_ptr<ArrayConverter<I, O>>> child_converters_;
};

// template <typename I, typename O,
//           template <typename, typename, typename> class TAC,
//           template <typename, typename, typename> class LAC,
//           template <typename, typename, typename> class SAC>
// Result<std::shared_ptr<ArrayConverter<I, O>>> MakeArrayConverter(
//     std::shared_ptr<DataType> type, MemoryPool* pool, O options);

// TODO: pass optional listconverter and typed converter classes as template args
template <typename I, typename O, template <typename, typename, typename> class TAC,
          template <typename, typename, typename> class LAC,
          template <typename, typename, typename> class SAC>
struct ArrayConverterBuilder {
  using Self = ArrayConverterBuilder<I, O, TAC, LAC, SAC>;
  using ArrayConverterPtr = std::shared_ptr<ArrayConverter<I, O>>;

  Status Visit(const NullType& t) {
    // TODO: merge with the primitive c_type variant below
    using T = NullType;
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using TypedConverter = TAC<T, I, O>;
    auto builder = std::make_shared<BuilderType>(pool);
    out->reset(new TypedConverter(std::move(builder), options));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<!is_nested_type<T>::value && !is_interval_type<T>::value &&
                  !is_dictionary_type<T>::value && !is_extension_type<T>::value,
              Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using TypedConverter = TAC<T, I, O>;
    auto builder = std::make_shared<BuilderType>(type, pool);
    out->reset(new TypedConverter(std::move(builder), options));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_list_like_type<T>::value && !std::is_same<T, MapType>::value, Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ListConverter = LAC<T, I, O>;
    ARROW_ASSIGN_OR_RAISE(auto child_converter,
                          (Self::Make(t.value_type(), pool, options)));
    auto builder = std::make_shared<BuilderType>(pool, child_converter->builder(), type);
    out->reset(
        new ListConverter(std::move(builder), std::move(child_converter), options));
    return Status::OK();
  }

  Status Visit(const StructType& t) {
    using T = StructType;
    using StructConverter = SAC<T, I, O>;

    ArrayConverterPtr child_converter;
    std::vector<ArrayConverterPtr> child_converters;
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;

    for (const auto& field : t.fields()) {
      ARROW_ASSIGN_OR_RAISE(child_converter, (Self::Make(field->type(), pool, options)));

      // TODO: use move
      child_converters.emplace_back(child_converter);
      child_builders.emplace_back(child_converter->builder());
    }

    auto builder = std::make_shared<StructBuilder>(type, pool, child_builders);
    out->reset(
        new StructConverter(std::move(builder), std::move(child_converters), options));
    return Status::OK();
  }

  Status Visit(const DataType& t) { return Status::NotImplemented(t.name()); }

  static Result<ArrayConverterPtr> Make(std::shared_ptr<DataType> type, MemoryPool* pool,
                                        O options) {
    ArrayConverterPtr out;
    Self visitor = {type, pool, options, &out};
    RETURN_NOT_OK(VisitTypeInline(*type, &visitor));
    return out;
  }

  const std::shared_ptr<DataType>& type;
  MemoryPool* pool;
  O options;
  ArrayConverterPtr* out;
};

}  // namespace arrow
