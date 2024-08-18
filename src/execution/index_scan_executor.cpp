//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
   // Obtain the index from the catalog
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

  // Create a key for the index scan from the predicate keys
  if (!plan_->pred_keys_.empty()) {
    // Create a dummy Tuple to use for evaluation
    Schema key_schema = *index_->index_->GetKeySchema();
    Tuple dummy_tuple;
    auto expr = plan_->pred_keys_.front();
    auto value = expr->Evaluate(&dummy_tuple, key_schema);  // Provide a valid Schema
    key_ = Tuple({value}, index_->index_->GetKeySchema());
  }

  // Perform the index scan to get RIDs
  index_->index_->ScanKey(key_, &rids_, exec_ctx_->GetTransaction());

  // Set up the iterator for the results
  index_iter_ = rids_.begin();
  index_iter_end_ = rids_.end();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (index_iter_ == index_iter_end_) {
    return false;
  }

  *rid = *index_iter_;
  index_iter_++;

  // Fetch the tuple from the table heap
  auto tupleinfo = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid);
  *tuple = tupleinfo.second;

  return true;    
}

}  // namespace bustub
