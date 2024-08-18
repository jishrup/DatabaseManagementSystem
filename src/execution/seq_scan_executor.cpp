//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),  table_info_(nullptr) {}

void SeqScanExecutor::Init() { 
  // Get the table information from the catalog
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  // Initialize the unique_ptr with a new TableIterator
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // Iterate through the table to find the next valid tuple
  while (!table_iter_->IsEnd()) {
    // Get the current tuple
    auto tup = table_iter_->GetTuple();
    *tuple = std::move(tup.second);
    *rid = tuple->GetRid();

    // Get the meta data of the tuple
    auto meta = std::move(tup.first);

    // Move to the next tuple in the table
    ++(*table_iter_);

    // Check if the tuple is deleted
    if (!meta.is_deleted_) {
      tuple = NULL;
      rid = NULL;
      return true;
    }
  }
  return false;  
}

}  // namespace bustub
