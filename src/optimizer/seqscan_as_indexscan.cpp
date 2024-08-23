#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // Check if the plan is a SeqScanPlanNode
  if (plan->GetType() != PlanType::SeqScan) {
    return plan;
  }

  // Cast the plan to SeqScanPlanNode
  auto seq_scan_plan = std::dynamic_pointer_cast<const SeqScanPlanNode>(plan);
  if (seq_scan_plan == nullptr) {
    return plan;
  }

  // Get the filter predicate
  auto filter_predicate = seq_scan_plan->filter_predicate_;
  if (filter_predicate == nullptr) {
    return plan;  // No predicate, so no optimization possible
  }

  // Extract the column index from the predicate if it is an equality comparison
  auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(filter_predicate->GetChildAt(0));
  auto const_expr = std::dynamic_pointer_cast<ConstantValueExpression>(filter_predicate->GetChildAt(1));
  
  if (column_expr == nullptr || const_expr == nullptr) {
    return plan;  // Not a simple equality predicate, so no optimization possible
  }

  auto table_name = seq_scan_plan->table_name_;
  auto column_idx = column_expr->GetColIdx();

  // Check if there is an index on the column
  auto index_info = MatchIndex(table_name, column_idx);
  if (!index_info.has_value()) {
    return plan;  // No suitable index found
  }

  // Extract the index_oid and index name
  auto [index_oid, index_name] = index_info.value();

  // Create a vector of predicate keys
  std::vector<AbstractExpressionRef> pred_keys = {const_expr};

  // Create an IndexScanPlanNode
  return std::make_shared<IndexScanPlanNode>(plan->output_schema_, seq_scan_plan->table_oid_, index_oid,
                                             filter_predicate, std::move(pred_keys));
}

}  // namespace bustub
