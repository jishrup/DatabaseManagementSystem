//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { 
    // Initialize the child executor to fetch the tuples to be deleted
    child_executor_->Init();
 }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    int32_t deleted_count = 0;
    Tuple child_tuple;
    RID child_rid;

    // Iterate over the child executor to delete tuples
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        // Fetch the table information
        auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

        // Mark the tuple as deleted in the table
        auto meta = table_info->table_->GetTupleMeta(child_rid);
        meta.is_deleted_ = true;
        table_info->table_->UpdateTupleMeta(meta, child_rid);

        // Update all indexes by removing the deleted tuple
        for (const auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
            auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            index_info->index_->DeleteEntry(key, child_rid, exec_ctx_->GetTransaction());
        }

        // Increment the deleted count
        deleted_count++;
    }

    // After the iteration, return the number of deleted rows as the result
    if (deleted_count > 0) {
        std::vector<Value> values{ValueFactory::GetIntegerValue(deleted_count)};
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
    }

    return false;  // No more tuples to delete
 }

}  // namespace bustub
