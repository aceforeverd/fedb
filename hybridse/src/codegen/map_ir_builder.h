/*
 * Copyright 2022 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_CODEGEN_MAP_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_MAP_IR_BUILDER_H_

#include "codegen/struct_ir_builder.h"

namespace hybridse {
namespace codegen {

class MapIRBuilder final : public StructTypeIRBuilder {
 public:
    MapIRBuilder(::llvm::Module* m, ::llvm::Type* key_ty, ::llvm::Type* value_ty);
    ~MapIRBuilder() override {}

    absl::StatusOr<NativeValue> Construct(CodeGenContext* ctx, absl::Span<const NativeValue> args) const override;

    bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist) override { return true; }
    base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src, NativeValue* output) override {
        return {};
    }

    absl::StatusOr<NativeValue> ExtractElement(CodeGenContext* ctx, const NativeValue&,
                                               const NativeValue&) const override;

    absl::StatusOr<NativeValue> MapKeys(CodeGenContext*, const NativeValue&) const;

 private:
    void InitStructType() override;

    bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) override;

 private:
    ::llvm::Type* key_type_ = nullptr;
    ::llvm::Type* value_type_ = nullptr;
};

}  // namespace codegen
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_CODEGEN_MAP_IR_BUILDER_H_
