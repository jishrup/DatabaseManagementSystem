//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void UpdateExecutor::Init() { 
  // Initialize the child executor
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  //std::cout<<"Update"<<std::endl;
  int updated_count = 0;
  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
      // 1. Get the existing tuple meta and mark it for deletion.
      auto [old_meta, old_tuple] = table_info_->table_->GetTuple(child_rid);
      old_meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(old_meta, child_rid);

      // 2. Create a new tuple based on the update expressions.
      std::vector<Value> values;
      const Schema output_schema = (const Schema)table_info_->schema_;
      for (const auto &expr : plan_->target_expressions_) {
          values.push_back(expr->Evaluate(&child_tuple, output_schema));
      }
      Tuple new_tuple(values, &table_info_->schema_);

      // 3. Insert the new tuple into the table.
      TupleMeta new_meta{0 /* transaction id */, false /* is_deleted */};
      auto insert_rid = table_info_->table_->InsertTuple(new_meta, new_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction());
      if (insert_rid.has_value()) {
          updated_count++;
      }

      // 4. Update any affected indexes.
      for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        // Remove old index entry
        auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());

        // Insert new index entry
        auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, *insert_rid, exec_ctx_->GetTransaction());
      }
  }

  // Prepare the output tuple indicating the number of rows updated
  auto output_schema = GetOutputSchema();
  std::vector<Value> values{Value(TypeId::INTEGER, updated_count)};
  *tuple = Tuple(values, &output_schema);


  return updated_count > 0;  
}

}  // namespace bustub
