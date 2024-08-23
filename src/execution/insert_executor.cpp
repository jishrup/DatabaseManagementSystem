//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    // Initialize the child executor
    child_executor_->Init(); 
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
     //std::cout<<"Insert"<<std::endl;
    int inserted_count = 0;

    // Iterate over tuples produced by the child executor
    Tuple child_tuple;
    RID child_rid;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        // Prepare TupleMeta for insertion
        TupleMeta meta{};
        meta.is_deleted_ = false;  // Insertion, so it's not deleted
        meta.ts_ = INVALID_TXN_ID; // Set the transaction ID to invalid as per instructions

        // Perform the insertion
        auto result = exec_ctx_->GetCatalog()
                          ->GetTable(plan_->GetTableOid())
                          ->table_->InsertTuple(meta, child_tuple, nullptr, exec_ctx_->GetTransaction(), plan_->GetTableOid());
        if (result.has_value()) {
            ++inserted_count;
        }
    }

    // Create a tuple to output the number of inserted rows
    auto output_schema = GetOutputSchema();
    std::vector<Value> values;
    values.emplace_back(Value(TypeId::INTEGER, inserted_count));
    auto tup = Tuple(values, &output_schema);
    *tuple = tup;

    //std::cout<<"see"<<tup.GetValue(&output_schema, 0).GetAs<int>()<<std::endl;
    //std::cout<<"see"<<tuple->GetValue(&output_schema, 0).GetAs<int>()<<std::endl;

    // Only return true once with the number of inserted rows
    return inserted_count > 0;
}

}  // namespace bustub
