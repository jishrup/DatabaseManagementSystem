//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  BUSTUB_ASSERT(max_depth <= HTABLE_DIRECTORY_MAX_DEPTH, "Invalid max_depth");

  max_depth_ = max_depth;
  global_depth_ = 0;

  for(uint32_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t { 
  return hash & ((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t { 
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");

  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");

  bucket_page_ids_[bucket_idx] =  bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  if(global_depth_ == 0) {
    return 0; 
  }

  return bucket_idx ^ (1 << (global_depth_ - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return (1 << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");

  return (1 << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { 
  return global_depth_;  
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
  return max_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  BUSTUB_ASSERT(global_depth_ < max_depth_, "global depth cannot be greater than max depth");

  global_depth_++;

  if(global_depth_ >= 1) {
    int val = 1 << (global_depth_ - 1);

    for(int i = val; i < 1 << global_depth_; i++) {
      bucket_page_ids_[i] = bucket_page_ids_[i % val];
      local_depths_[i] = local_depths_[i % val];
    }
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  global_depth_--;

  BUSTUB_ASSERT(global_depth_ >= 0, "global depth cannot be less than 0");
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool { 
  for (uint32_t i = 0; i < Size(); ++i) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { 
  return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  return HTABLE_DIRECTORY_ARRAY_SIZE;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t { 
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");

  return local_depths_[bucket_idx];  
}

auto ExtendibleHTableDirectoryPage::GetSplitIndex(uint32_t bucket_idx) const -> uint32_t { 
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");

  return bucket_idx | (1  << GetLocalDepth(bucket_idx));  
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");
  
  local_depths_[bucket_idx] = local_depth; 
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");
  BUSTUB_ASSERT(local_depths_[bucket_idx] + 1 <= max_depth_, "Cant increment local depth for this bucket_idx");

  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Invalid bucket_idx");
  BUSTUB_ASSERT(local_depths_[bucket_idx] - 1 > 0, "Cant decrement local depth for this bucket_idx");

  local_depths_[bucket_idx]--;
}

}  // namespace bustub
