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

#include <algorithm>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/key_hash_internal.h"
#include "arrow/compute/light_array_internal.h"
#include "arrow/compute/util.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

// Function documentation
const FunctionDoc hash_64_doc{
    "Construct a hash for every element of the input argument",
    ("An element-wise function that uses an xxHash-like algorithm.\n"
     "This function is not suitable for cryptographic purposes.\n"
     "Hash results are 64-bit and emitted for each valid row.\n"
     "Null (or invalid) rows emit a null in the output."),
    {"hash_input"}};

// ------------------------------
// Kernel implementations
// It is expected that HashArrowType is either UInt32Type or UInt64Type (default)
// template <typename HashArrowType = UInt64Type>
struct FastHashScalar {
  static Result<KeyColumnArray> ToColumnArray(
      const ArraySpan& array, LightContext* ctx,
      const uint8_t* list_values_buffer = nullptr) {
    KeyColumnMetadata metadata;
    const uint8_t* validity_buffer = nullptr;
    const uint8_t* fixed_length_buffer = nullptr;
    const uint8_t* var_length_buffer = nullptr;

    if (array.GetBuffer(0) != nullptr) {
      validity_buffer = array.GetBuffer(0)->data();
    }
    if (array.GetBuffer(1) != nullptr) {
      fixed_length_buffer = array.GetBuffer(1)->data();
    }

    auto type_id = array.type->id();
    if (type_id == Type::NA) {
      metadata = KeyColumnMetadata(true, 0, true);
    } else if (type_id == Type::BOOL) {
      metadata = KeyColumnMetadata(true, 0);
    } else if (is_fixed_width(type_id)) {
      metadata = KeyColumnMetadata(true, array.type->bit_width() / 8);
    } else if (is_binary_like(type_id)) {
      metadata = KeyColumnMetadata(false, sizeof(uint32_t));
      var_length_buffer = array.GetBuffer(2)->data();
    } else if (is_large_binary_like(type_id)) {
      metadata = KeyColumnMetadata(false, sizeof(uint64_t));
      var_length_buffer = array.GetBuffer(2)->data();
    } else if (type_id == Type::MAP) {
      metadata = KeyColumnMetadata(false, sizeof(uint32_t));
      var_length_buffer = list_values_buffer;
    } else if (type_id == Type::LIST) {
      metadata = KeyColumnMetadata(false, sizeof(uint32_t));
      var_length_buffer = list_values_buffer;
    } else if (type_id == Type::LARGE_LIST) {
      metadata = KeyColumnMetadata(false, sizeof(uint64_t));
      var_length_buffer = list_values_buffer;
    } else if (type_id == Type::FIXED_SIZE_LIST) {
      auto list_type = checked_cast<const FixedSizeListType*>(array.type);
      metadata = KeyColumnMetadata(true, list_type->list_size());
      fixed_length_buffer = list_values_buffer;
    } else {
      return Status::TypeError("Unsupported column data type ", array.type->name(),
                               " used with hash_64 compute kernel");
    }

    return KeyColumnArray(metadata, array.length, validity_buffer, fixed_length_buffer,
                          var_length_buffer);
  }

  static Result<std::shared_ptr<ArrayData>> HashChild(const ArraySpan& array,
                                                      const ArraySpan& child,
                                                      LightContext* hash_ctx,
                                                      MemoryPool* memory_pool) {
    auto buffer_size = child.length * sizeof(uint64_t);
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(buffer_size, memory_pool));
    ARROW_RETURN_NOT_OK(
        HashArray(child, hash_ctx, memory_pool, buffer->mutable_data_as<uint64_t>()));
    return ArrayData::Make(uint64(), child.length,
                           {array.GetBuffer(0), std::move(buffer)}, array.null_count);
  }

  static Status HashArray(const ArraySpan& array, LightContext* hash_ctx,
                          MemoryPool* memory_pool, uint64_t* out) {
    // KeyColumnArray objects are being passed to the hashing utility
    std::vector<KeyColumnArray> columns(1);
    // ensure that we keep the converted child arrays alive because KeyColumnArray
    // only provides a view into the original array data
    std::vector<std::shared_ptr<ArrayData>> children;

    auto type_id = array.type->id();
    if (type_id == Type::STRUCT) {
      columns.reserve(array.child_data.size());
      for (size_t i = 0; i < array.child_data.size(); i++) {
        auto child = array.child_data[i];
        if (is_nested(child.type->id())) {
          ARROW_ASSIGN_OR_RAISE(auto child_hashes,
                                HashChild(array, child, hash_ctx, memory_pool));
          children.push_back(child_hashes);
          ARROW_ASSIGN_OR_RAISE(columns[i], ToColumnArray(*child_hashes, hash_ctx));
        } else {
          ARROW_ASSIGN_OR_RAISE(columns[i], ToColumnArray(child, hash_ctx));
        }
      }
    } else if (is_list_like(type_id)) {
      auto values = array.child_data[0];
      ARROW_ASSIGN_OR_RAISE(auto value_hashes,
                            HashChild(array, values, hash_ctx, memory_pool));
      children.push_back(value_hashes);
      ARROW_ASSIGN_OR_RAISE(
          columns[0], ToColumnArray(array, hash_ctx, value_hashes->buffers[1]->data()));
    } else {
      ARROW_ASSIGN_OR_RAISE(columns[0], ToColumnArray(array, hash_ctx));
    }
    Hashing64::HashMultiColumn(columns, hash_ctx, out);
    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    if (input_arg.num_values() != 1 || !input_arg[0].is_array()) {
      return Status::Invalid("FastHash currently supports a single array input");
    }
    ArraySpan hash_input = input_arg[0].array;

    auto exec_ctx = default_exec_context();
    if (ctx && ctx->exec_context()) {
      exec_ctx = ctx->exec_context();
    }

    // Initialize stack-based memory allocator used by Hashing32 and Hashing64
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(
        stack_memallocator.Init(exec_ctx->memory_pool(),
                                3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

    // Prepare context used by Hashing32 and Hashing64
    LightContext hash_ctx;
    hash_ctx.hardware_flags = exec_ctx->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;

    // Call the hashing function, overloaded based on OutputCType
    ArraySpan* result_span = out->array_span_mutable();
    uint64_t* result_ptr = result_span->GetValues<uint64_t>(1);
    ARROW_RETURN_NOT_OK(
        HashArray(hash_input, &hash_ctx, exec_ctx->memory_pool(), result_ptr));

    return Status::OK();
  }
};

// ------------------------------
// Function construction and kernel registration
std::shared_ptr<ScalarFunction> RegisterKernelsFastHash64() {
  // Create function instance
  auto fn_hash_64 =
      std::make_shared<ScalarFunction>("hash_64", Arity::Unary(), hash_64_doc);

  // Add 64-bit hash kernel
  ScalarKernel kernel({InputType()}, OutputType(uint64()), FastHashScalar::Exec);
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  DCHECK_OK(fn_hash_64->AddKernel(std::move(kernel)));

  return fn_hash_64;
}

}  // namespace

void RegisterScalarHash(FunctionRegistry* registry) {
  auto fn_scalarhash64 = RegisterKernelsFastHash64();
  DCHECK_OK(registry->AddFunction(std::move(fn_scalarhash64)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
