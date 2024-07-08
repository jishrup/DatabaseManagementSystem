//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  
  // Initialize the memeber variables
  auto header_pg = BasicPageGuard(bpm, bpm->NewPage(&header_page_id_));
  header_page_guard_ =  header_pg.UpgradeWrite();
  header_page_ = header_page_guard_.AsMut<ExtendibleHTableHeaderPage>();
  header_page_->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  WritePageGuard directory_guard;
  const ExtendibleHTableDirectoryPage *directory;

  // Hash the key
  uint32_t hash = Hash(key);

  // Fetch the directory page
  auto directory_page_id = (page_id_t)header_page_->GetDirectoryPageId(header_page_->HashToDirectoryIndex(hash));

  // Fetch directory
  if(directory_page_id != INVALID_PAGE_ID) {
    directory_guard = bpm_->FetchPageWrite(directory_page_id);
    directory = directory_guard.As<ExtendibleHTableDirectoryPage>();
  } else {
    return false;
  }

  // Calculate the bucket index
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  // fetch the bucket
  WritePageGuard bucket_guard =  bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // walk through the bucket and find the values associated with this key
  for (uint32_t i = 0; i < bucket->Size(); ++i) {
    auto kv = bucket->EntryAt(i);
    if(cmp_(kv.first, key) == 0) {
      result->push_back(kv.second);
    }
  }

  if(result->size() == 0) {
    return false;
  } else {
    return true;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  WritePageGuard directory_guard;
  ExtendibleHTableDirectoryPage *directory;
  ExtendibleHTableBucketPage<K, V, KC> * bucket;
  ExtendibleHTableBucketPage<K, V, KC> * new_bucket;

  // Hash the key
  uint32_t hash = Hash(key);

  // Fetch the directory page
  auto directory_page_id = (page_id_t)header_page_->GetDirectoryPageId(header_page_->HashToDirectoryIndex(hash));

  // Fetch directory
  if(directory_page_id != INVALID_PAGE_ID) {
    directory_guard = bpm_->FetchPageWrite(directory_page_id);
    directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  } else {
    BasicPageGuard bdirectory_guard = bpm_->NewPageGuarded(&directory_page_id);
    directory_guard = bdirectory_guard.UpgradeWrite();
    directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory->Init(directory_max_depth_);
    header_page_->SetDirectoryPageId(header_page_->HashToDirectoryIndex(hash), directory_page_id);
  }

  // Calculate the bucket index
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  // Allocate new bucket Page if not allocated
  WritePageGuard bucket_guard;
  if(bucket_page_id == INVALID_PAGE_ID) {
    BasicPageGuard bbucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
    bucket_guard = bbucket_guard.UpgradeWrite();
    bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
  } else {
    bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }

  // Check if the bucket is full
  if (bucket->IsFull()) {
    //Increment the local depth
    uint32_t new_bucket_idx = directory->GetSplitIndex(bucket_idx);
    page_id_t new_bucket_page_id =  INVALID_PAGE_ID;

    // for the buckets where global depth need not be increased else Increase the global depth
    if(new_bucket_idx < directory->Size()) {
      directory->IncrLocalDepth(bucket_idx);
      directory->IncrLocalDepth(new_bucket_idx);
    } else {
      // Return false if the maximum capacity for this directory is reached
      if(directory->GetGlobalDepth() == directory->GetMaxDepth()) {
        return false;
      }

      directory->IncrLocalDepth(bucket_idx);
      directory->IncrGlobalDepth();
    }

    // Create a new bucket
    BasicPageGuard new_bbucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
    WritePageGuard new_bucket_guard = new_bbucket_guard.UpgradeWrite();
    new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

    // Re-distribute entries between the original and new bucket
    for (uint32_t i = 0; i < bucket->Size(); ++i) {
      auto kv = bucket->EntryAt(i);
      uint32_t new_hash = Hash(kv.first);
      uint32_t verynew_bucket_idx = directory->HashToBucketIndex(new_hash);
      if (new_bucket_idx == verynew_bucket_idx) {
        new_bucket->Insert(kv.first, kv.second, cmp_);
        bucket->RemoveAt(i);
      }
    }

    if(directory->HashToBucketIndex(hash) == new_bucket_idx) {
      new_bucket->Insert(key, value, cmp_);
    } else {
      bucket->Insert(key, value, cmp_);
    }
  } else {
    // the case where no values where redistributed to the new bucket
    if(bucket->IsFull()) {
      Insert(key, value, transaction);
    } else {
      bucket->Insert(key, value, cmp_);
    }
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  WritePageGuard directory_guard;
  ExtendibleHTableDirectoryPage *directory;
  bool found = false;

  // Hash the key
  uint32_t hash = Hash(key);

  // Fetch the directory page
  auto directory_page_id = (page_id_t)header_page_->GetDirectoryPageId(header_page_->HashToDirectoryIndex(hash));

  // Fetch directory
  if(directory_page_id != INVALID_PAGE_ID) {
    directory_guard = bpm_->FetchPageWrite(directory_page_id);
    directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  } else {
    return false;
  }

  // Calculate the bucket index
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  // fetch the bucket
  WritePageGuard bucket_guard =  bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // walk through the bucket and find the values associated with this key
  for (uint32_t i = 0; i < bucket->Size(); ++i) {
    auto kv = bucket->EntryAt(i);
    if(cmp_(kv.first, key) == 0) {
      found = true;
      bucket->Remove(key, cmp_);
      i--;
    }
  }

  // if the element is not found return false
  if(!found) {
    return false;
  }

  if(bucket->IsEmpty()) {
    if(directory->GetGlobalDepth() > 1) {
      int val = 1 << (directory->GetGlobalDepth() - 1);

      // assign current bucket to its lower version and decrement the local depth for both
      directory->SetBucketPageId(bucket_idx, directory->GetBucketPageId(bucket_idx % val));
      directory->DecrLocalDepth(bucket_idx);
      directory->DecrLocalDepth(bucket_idx % val);

      if(directory->CanShrink()) {
        directory->DecrGlobalDepth();
      }
    } else if(directory->GetGlobalDepth() == 1) {
      directory->DecrGlobalDepth();
      directory->SetLocalDepth(0, 0);
    }
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
