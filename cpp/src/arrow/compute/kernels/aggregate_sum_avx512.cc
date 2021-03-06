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

#include "arrow/compute/kernels/aggregate_basic_internal.h"

namespace arrow {
namespace compute {
namespace aggregate {

// ----------------------------------------------------------------------
// Sum implementation

// Round size optimized based on data type and compiler
template <typename T>
struct RoundSizeAvx512 {
  static constexpr int64_t size = 32;
};

// Round size set to 64 for float/int32_t/uint32_t
template <>
struct RoundSizeAvx512<float> {
  static constexpr int64_t size = 64;
};

template <>
struct RoundSizeAvx512<int32_t> {
  static constexpr int64_t size = 64;
};

template <>
struct RoundSizeAvx512<uint32_t> {
  static constexpr int64_t size = 64;
};

template <typename ArrowType>
struct SumImplAvx512
    : public SumImpl<RoundSizeAvx512<typename TypeTraits<ArrowType>::CType>::size,
                     ArrowType> {};

template <typename ArrowType>
struct MeanImplAvx512
    : public MeanImpl<RoundSizeAvx512<typename TypeTraits<ArrowType>::CType>::size,
                      ArrowType> {};

std::unique_ptr<KernelState> SumInitAvx512(KernelContext* ctx,
                                           const KernelInitArgs& args) {
  SumLikeInit<SumImplAvx512> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInitAvx512(KernelContext* ctx,
                                            const KernelInitArgs& args) {
  SumLikeInit<MeanImplAvx512> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

}  // namespace aggregate

namespace internal {

void RegisterScalarAggregateSumAvx512(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary());
  aggregate::AddBasicAggKernels(aggregate::SumInitAvx512, {boolean()}, int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInitAvx512, SignedIntTypes(), int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInitAvx512, UnsignedIntTypes(), uint64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInitAvx512, FloatingPointTypes(), float64(),
                                func.get());
  // Register the override AVX512 version
  DCHECK_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary());
  aggregate::AddBasicAggKernels(aggregate::MeanInitAvx512, {boolean()}, float64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::MeanInitAvx512, NumericTypes(), float64(),
                                func.get());
  // Register the override AVX512 version
  DCHECK_OK(registry->AddFunction(std::move(func), /*allow_overwrite=*/true));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
