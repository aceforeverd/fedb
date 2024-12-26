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
#include "passes/physical/window_union_rewrite_optimized.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "codegen/ir_base_builder.h"

namespace hybridse {
namespace passes {

bool WindowUnionOptimized::Transform(PhysicalOpNode* in, PhysicalOpNode** output) {
    *output = in;
    if (in->GetOpType() != vm::kPhysicalOpLimit) {
        return false;
    }

    auto limit_op = in->GetAsOrNull<vm::PhysicalLimitNode>();
    assert(limit_op);
    if (limit_op->GetLimitCnt() != 1) {
        // 1. limit 1
        return false;
    }

    if (in->GetProducerCnt() == 0 || in->GetProducer(0)->GetOpType() != vm::kPhysicalOpSortBy) {
        return false;
    }

    // order by col1 desc
    auto sort_op = in->GetProducer(0)->GetAsOrNull<vm::PhysicalSortNode>();
    assert(sort_op);
    auto ordering_expr_list = sort_op->sort().orders()->order_expressions();
    if (ordering_expr_list->GetChildNum() != 1) {
        return false;
    }
    auto ordering_expr = ordering_expr_list->GetChild(0)->GetAsOrNull<node::OrderExpression>();
    if (ordering_expr->is_asc()) {
        return false;
    }
    auto ordering_column = ordering_expr->expr()->GetAsOrNull<node::ColumnRefNode>();
    if (!ordering_column) {
        return false;
    }

    auto sort_input = sort_op->GetProducer(0);
    if (sort_input->GetOpType() != vm::kPhysicalOpProject) {
        return false;
    }
    auto win_agg_node = sort_input->GetAsOrNull<vm::PhysicalWindowAggrerationNode>();
    if (!win_agg_node) {
        return false;
    }

    // one window partition key
    std::vector<const node::ExprNode*> win_partition_key_list;
    win_agg_node->window().partition().ResolvedRelatedColumns(&win_partition_key_list);
    if (win_partition_key_list.size() != 1) {
        return false;
    }
    auto win_partition_key = win_partition_key_list.front()->GetAsOrNull<node::ColumnRefNode>();
    if (!win_partition_key) {
        return false;
    }

    // one window order by key
    std::vector<const node::ExprNode*> win_ordering_key_list;
    win_agg_node->window().sort().ResolvedRelatedColumns(&win_ordering_key_list);
    if (win_ordering_key_list.size() != 1) {
        return false;
    }
    auto win_ordering_key = win_ordering_key_list.front()->GetAsOrNull<node::ColumnRefNode>();
    if (!win_ordering_key) {
        return false;
    }

    const node::ColumnRefNode* join_left_key = nullptr;
    const node::ColumnRefNode* join_right_key = nullptr;
    vm::PhysicalOpNode* join_left_source = nullptr;
    vm::PhysicalOpNode* join_right_source = nullptr;
    // left join should be win agg node's input
    auto project_in = win_agg_node->GetProducer(0);
    if (project_in->GetOpType() == vm::kPhysicalOpJoin) {
        auto join_op = project_in->GetAsOrNull<vm::PhysicalJoinNode>();
        assert(join_op);
        join_left_source = join_op->GetProducer(0);
        join_right_source = join_op->GetProducer(1);

        if (!isConstSource(join_left_source)) {
            // left source is a const query, output 1 row
            // NOTE(xxx): this can be relaxed
            return false;
        }

        if (join_op->join().join_type_ != node::JoinType::kJoinTypeLeft) {
            return false;
        }
        if (!join_op->join().left_key().ValidKey() || !join_op->join().right_key().ValidKey()) {
            // run ConditionOptimized first
            // left key & right key should exists
            return false;
        }

        std::vector<const node::ExprNode*> join_left_key_list;
        join_op->join().left_key().ResolvedRelatedColumns(&join_left_key_list);
        if (join_left_key_list.size() != 1) {
            return false;
        }
        join_left_key = join_left_key_list.front()->GetAsOrNull<node::ColumnRefNode>();
        if (!join_left_key) {
            return false;
        }

        std::vector<const node::ExprNode*> join_right_key_list;
        join_op->join().right_key().ResolvedRelatedColumns(&join_right_key_list);
        if (join_right_key_list.size() != 1) {
            return false;
        }
        join_right_key = join_left_key_list.front()->GetAsOrNull<node::ColumnRefNode>();
        if (!join_right_key) {
            return false;
        }
    } else {
        // TODO(someone): handle request join
        return false;
    }

    auto as = resolveColumnToSource(project_in, ordering_column);
    if (!as.ok()) {
        return false;
    }
    auto ordering_id = as.value();

    as = resolveColumnToSource(project_in, win_partition_key);
    if (!as.ok()) {
        return false;
    }
    size_t win_partition_id = as.value();

    as = resolveColumnToSource(project_in, win_ordering_key);
    if (!as.ok()) {
        return false;
    }
    size_t win_order_id = as.value();

    as = resolveColumnToSource(project_in, join_left_key);
    if (!as.ok()) {
        return false;
    }
    auto join_left_key_id = as.value();

    as = resolveColumnToSource(project_in, join_right_key);
    if (!as.ok()) {
        return false;
    }
    auto join_right_key_id = as.value();

    // 2. sort column same to window order by column
    if (ordering_id != win_order_id) {
        return false;
    }

    // 3. left join condition column (either one) same to window partition by column
    if (join_left_key_id != win_partition_id && join_right_key_id != win_partition_id) {
        return false;
    }

    // matches all, rewriting

    vm::ColumnProjects left_selects;
    vm::ColumnProjects right_selects;
    auto nm = plan_ctx_->node_manager();

    std::vector<const node::ExprNode*> related_cols;
    related_cols.push_back(win_partition_key);
    related_cols.push_back(win_ordering_key);
    win_agg_node->project().ResolvedRelatedColumns(&related_cols);

    std::unordered_set<std::string> visited_cols;
    for (auto expr : related_cols) {
        auto ref = expr->GetAsOrNull<node::ColumnRefNode>();
        assert(ref);

        if (visited_cols.count(ref->GetExprString()) == 1) {
            continue;
        }

        size_t sc_idx, col_idx;
        auto s = project_in->schemas_ctx()->ResolveColumnRefIndex(ref, &sc_idx, &col_idx);
        if (!s.isOK()) {
            return false;
        }
        node::DataType data_type;
        if (!codegen::SchemaType2DataType(project_in->schemas_ctx()->GetSchemaSource(sc_idx)->GetColumnType(col_idx),
                                          &data_type)) {
            return false;
        }
        if (sc_idx == 0) {
            // to left source
            left_selects.Add(ref->GetColumnName(), nm->MakeColumnRefNode(ref->GetColumnName(), ref->GetRelationName()),
                             nullptr);
            right_selects.Add(ref->GetColumnName(), nm->MakeCastNode(data_type, nm->MakeConstNode()), nullptr);
        } else {
            // to right source
            left_selects.Add(ref->GetColumnName(), nm->MakeCastNode(data_type, nm->MakeConstNode()), nullptr);
            right_selects.Add(ref->GetColumnName(), nm->MakeColumnRefNode(ref->GetColumnName(), ref->GetRelationName()),
                              nullptr);
        }

        visited_cols.insert(ref->GetExprString());
    }

    vm::PhysicalSimpleProjectNode* new_left_node = nullptr;
    vm::PhysicalSimpleProjectNode* new_right_node = nullptr;
    auto ss = plan_ctx_->CreateOp(&new_left_node, join_left_source, left_selects);
    if (!ss.isOK()) {
        LOG(INFO) << ss;
        return false;
    }
    ss = plan_ctx_->CreateOp(&new_right_node, join_right_source, right_selects);
    if (!ss.isOK()) {
        LOG(INFO) << ss;
        return false;
    }

    vm::PhysicalWindowAggrerationNode* new_win_agg_node = nullptr;
    ss = plan_ctx_->CreateOp(&new_win_agg_node, new_left_node, win_agg_node->project(), win_agg_node->window(), true,
                             win_agg_node->need_append_input(), win_agg_node->exclude_current_time());
    new_win_agg_node->window().range().frame()->exclude_current_row_ = true;
    if (!ss.isOK()) {
        LOG(INFO) << ss;
        return false;
    }

    if (!new_win_agg_node->AddWindowUnion(new_right_node)) {
        return false;
    }
    *output = new_win_agg_node;

    return true;
}
absl::StatusOr<size_t> WindowUnionOptimized::resolveColumnToSource(vm::PhysicalOpNode* in,
                                                                   const node::ColumnRefNode* ref) {
    size_t id;
    auto s = in->schemas_ctx()->ResolveColumnID(ref->GetDBName(), ref->GetRelationName(), ref->GetColumnName(), &id);
    if (!s.isOK()) {
        return absl::FailedPreconditionError(s.msg);
    }
    return id;
}
bool WindowUnionOptimized::isConstSource(const PhysicalOpNode* in) {
    if (in->GetOpType() == vm::kPhysicalOpConstProject) {
        return true;
    }

    if (in->GetOpType() == vm::kPhysicalOpRename) {
        return isConstSource(in->GetProducer(0));
    }
    return false;
}
}  // namespace passes
}  // namespace hybridse
