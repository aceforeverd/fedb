/*
 * Copyright 2021 4paradigm
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
#include "passes/physical/simple_project_optimized.h"
#include <type_traits>
#include <vector>
#include "vm/physical_op.h"

namespace hybridse {
namespace passes {

static Status BuildColumnMapping(
    const node::ExprNode* outer_expr,
    const std::vector<node::ExprNode*>& inner_projects,
    const vm::SchemasContext* schemas_ctx, passes::ExprReplacer* replacer);

// inline consecutive nodes impl in template
template <typename OuterNode, typename InnerNode>
absl::StatusOr<vm::PhysicalOpNode*> InlineNodeImpl(vm::PhysicalPlanContext* plan_ctx, OuterNode* outer,
                                                   InnerNode* inner, const vm::SchemasContext* sc) {
    auto nm = plan_ctx->node_manager();

    auto outer_projects = outer->project();
    std::vector<node::ExprNode*> outer_exprs;
    for (size_t i = 0; i < outer_projects.size(); ++i) {
        outer_exprs.push_back(outer_projects.GetExpr(i)->DeepCopy(nm));
    }

    auto inner_projects = inner->project();
    std::vector<node::ExprNode*> inner_exprs;
    for (size_t i = 0; i < inner_projects.size(); ++i) {
        inner_exprs.push_back(inner_projects.GetExpr(i)->DeepCopy(nm));
    }

    Status status;
    passes::ExprReplacer replacer;
    for (size_t i = 0; i < outer_projects.size(); ++i) {
        status = BuildColumnMapping(outer_exprs[i], inner_exprs, sc, &replacer);
        if (!status.isOK()) {
            return absl::FailedPreconditionError(absl::StrCat("Fail to merge simple projects: ", status.msg));
        }
    }

    vm::ColumnProjects new_projects;
    for (size_t i = 0; i < outer_projects.size(); ++i) {
        node::ExprNode* new_expr;
        status = replacer.Replace(outer_exprs[i], &new_expr);
        if (!status.isOK()) {
            return absl::FailedPreconditionError(absl::StrCat("Fail to merge simple projects: ", status.msg));
        }
        new_projects.Add(outer_projects.GetName(i), new_expr, outer_projects.GetFrame(i));
    }

    if constexpr (std::is_same_v<InnerNode, vm::PhysicalConstProjectNode>) {
        vm::PhysicalConstProjectNode* new_project_op = nullptr;
        status = plan_ctx->CreateOp<vm::PhysicalConstProjectNode>(&new_project_op, new_projects);
        if (!status.isOK()) {
            return absl::FailedPreconditionError(absl::StrCat("Fail to merge simple projects: ", status.msg));
        }
        return new_project_op;
    } else {
        vm::PhysicalRowProjectNode* new_project_op = nullptr;
        status = plan_ctx->CreateOp<vm::PhysicalRowProjectNode>(&new_project_op, inner->GetProducer(0), new_projects);
        if (!status.isOK()) {
            return absl::FailedPreconditionError(absl::StrCat("Fail to merge simple projects: ", status.msg));
        }
        return new_project_op;
    }
}

template <typename OuterNode, typename InnerNode>
bool INLINE_NODE_WRAP(vm::PhysicalPlanContext* plan_ctx, vm::PhysicalOpNode* outer, vm::PhysicalOpNode* inner,
                      vm::PhysicalOpNode** output) {
    auto s =
        InlineNodeImpl<OuterNode, InnerNode>(plan_ctx, outer->GetAsOrNull<OuterNode>(), inner->GetAsOrNull<InnerNode>(),
                                             outer->GetProducer(0)->schemas_ctx());
    if (!s.ok()) {
        LOG(WARNING) << s.status().ToString();
        return false;
    }
    *output = s.value();
    return true;
}

static bool transform_impl(vm::PhysicalPlanContext* plan_ctx, PhysicalOpNode* outer, PhysicalOpNode* inner,
                           const vm::SchemasContext* sc, PhysicalOpNode** output) {
    *output = outer;

    if (outer->GetProducerCnt() == 0) {
        return false;
    }

    switch (outer->GetOpType()) {
        case vm::kPhysicalOpSimpleProject: {
            switch (inner->GetOpType()) {
                case vm::kPhysicalOpSimpleProject: {
                    return INLINE_NODE_WRAP<vm::PhysicalSimpleProjectNode, vm::PhysicalSimpleProjectNode>(
                        plan_ctx, outer, inner, output);
                    break;
                }
                case vm::kPhysicalOpProject: {
                    auto pn = inner->GetAsOrNull<vm::PhysicalProjectNode>();
                    if (pn->project_type_ == vm::kRowProject) {
                        return INLINE_NODE_WRAP<vm::PhysicalSimpleProjectNode, vm::PhysicalRowProjectNode>(
                            plan_ctx, outer, inner, output);
                    }
                    break;
                }
                case vm::kPhysicalOpConstProject: {
                    return INLINE_NODE_WRAP<vm::PhysicalSimpleProjectNode, vm::PhysicalConstProjectNode>(
                        plan_ctx, outer, inner, output);
                    break;
                }
                case vm::kPhysicalOpRename: {
                    return transform_impl(plan_ctx, outer, inner->GetProducer(0), sc, output);
                }
                default:
                    break;
            }
            break;
        }
        case vm::kPhysicalOpProject: {
            auto outer_pn = outer->GetAsOrNull<vm::PhysicalProjectNode>();
            if (outer_pn->project_type_ != vm::kRowProject) {
                return false;
            }

            switch (inner->GetOpType()) {
                case vm::kPhysicalOpSimpleProject: {
                    return INLINE_NODE_WRAP<vm::PhysicalRowProjectNode, vm::PhysicalSimpleProjectNode>(plan_ctx, outer,
                                                                                                       inner, output);
                    break;
                }
                case vm::kPhysicalOpProject: {
                    auto pn = outer->GetProducer(0)->GetAsOrNull<vm::PhysicalProjectNode>();
                    if (pn->project_type_ == vm::kRowProject) {
                        return INLINE_NODE_WRAP<vm::PhysicalRowProjectNode, vm::PhysicalRowProjectNode>(plan_ctx, outer,
                                                                                                        inner, output);
                    }
                    break;
                }
                case vm::kPhysicalOpConstProject: {
                    return INLINE_NODE_WRAP<vm::PhysicalRowProjectNode, vm::PhysicalConstProjectNode>( plan_ctx, outer,
                                     inner, output);
                    break;
                }
                case vm::kPhysicalOpRename: {
                    return transform_impl(plan_ctx, outer, inner->GetProducer(0), sc, output);
                }
                default:
                    break;
            }
            break;
        }
        default:
            break;
    }
    return false;
}

bool SimpleProjectOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    if (in->GetProducerCnt() == 0) {
        return false;
    }
    return transform_impl(plan_ctx_, in, in->GetProducer(0), in->GetProducer(0)->schemas_ctx(), output);
}

static Status BuildColumnMapping(
    const node::ExprNode* outer_expr,
    const std::vector<node::ExprNode*>& inner_projects,
    const vm::SchemasContext* schemas_ctx, passes::ExprReplacer* replacer) {
    for (size_t i = 0; i < outer_expr->GetChildNum(); ++i) {
        CHECK_STATUS(BuildColumnMapping(outer_expr->GetChild(i), inner_projects,
                                        schemas_ctx, replacer));
    }
    switch (outer_expr->GetExprType()) {
        case node::kExprColumnRef: {
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(outer_expr);
            size_t schema_idx;
            size_t col_idx;
            schemas_ctx->ResolveColumnRefIndex(col_ref, &schema_idx, &col_idx);
            CHECK_TRUE(col_idx < inner_projects.size(), common::kPlanError,
                       "Column index out of bound");

            auto repl = inner_projects[col_idx];
            replacer->AddReplacement(col_ref, repl);
            break;
        }
        default:
            break;
    }
    return Status::OK();
}
}  // namespace passes
}  // namespace hybridse
