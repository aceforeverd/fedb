/**
 * Copyright (c) 2024 Ace <teapot@aceforeverd.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_WINDOW_UNION_REWRITE_OPTIMIZED_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_WINDOW_UNION_REWRITE_OPTIMIZED_H_

#include <absl/strings/string_view.h>
#include "node/sql_node.h"
#include "passes/physical/transform_up_physical_pass.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

class WindowUnionOptimized : public TransformUpPysicalPass {
 public:
    explicit WindowUnionOptimized(PhysicalPlanContext* plan_ctx) : TransformUpPysicalPass(plan_ctx) {}
    ~WindowUnionOptimized() {}

 private:
    // Limit (cnt = 1)
    //   Sort (ts desc)
    //     WindowAgg
    //       LeftJoin
    //         RequestSource
    //         UionSource
    bool Transform(PhysicalOpNode* in, PhysicalOpNode** output) override;

    // resolve ColumnRefNode under {in}'s schemas context
    absl::StatusOr<size_t> resolveColumnToSource(vm::PhysicalOpNode* in, const node::ColumnRefNode* ref);

    bool isConstSource(const PhysicalOpNode* in);
};
}  // namespace passes
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_WINDOW_UNION_REWRITE_OPTIMIZED_H_
