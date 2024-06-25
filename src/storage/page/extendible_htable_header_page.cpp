//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  BUSTUB_ASSERT(max_depth > HTABLE_HEADER_MAX_DEPTH, "Invalid max_depth");

  max_depth_ = max_depth;

  for(uint32_t i = 0; i < HTABLE_HEADER_ARRAY_SIZE; i++) {
    directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t { 
  return hash & (1 << max_depth_ -1);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  BUSTUB_ASSERT(directory_idx > (1 << max_depth_), "Invalid directory_idx");

  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  BUSTUB_ASSERT(directory_idx > (1 << max_depth_), "Invalid directory_idx");

  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { 
  return 1 << max_depth_;
}

}  // namespace bustub
