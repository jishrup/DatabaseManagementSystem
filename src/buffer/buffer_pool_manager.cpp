//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  // "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //"exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if (free_list_.size() > 0) {
    // get the first available free frame
    frame_id_t cur_frame = free_list_.front();
    free_list_.pop_front();

    // Allocate the page and assign this free frame the current page
    page_table_.insert({next_page_id_, cur_frame});
    pages_[cur_frame].page_id_ = next_page_id_;
    AllocatePage();

    // Record the access time
    replacer_->RecordAccess(cur_frame);

    // Pin the page
    pages_[cur_frame].pin_count_++;

    // return the current page
    *page_id = pages_[cur_frame].page_id_;
    return &pages_[cur_frame];
  } else {
    // get the evicted frame from LKU Replacer
    frame_id_t cur_frame;

    if (!replacer_->Evict(&cur_frame)) {
      page_id = nullptr;
      return nullptr;
    } else {
      // write this page to disk if its dirty
      if (pages_[cur_frame].is_dirty_) {
        auto promise1 = disk_scheduler_->CreatePromise();
        auto future1 = promise1.get_future();
        disk_scheduler_->Schedule(
            {/*is_write=*/true, pages_[cur_frame].data_, /*page_id=*/pages_[cur_frame].page_id_, std::move(promise1)});
        future1.get();
      }

      // remove the current page id from the page table
      page_table_.erase(pages_[cur_frame].page_id_);

      // clear the contents of this page and reset the metadata
      pages_[cur_frame].ResetMemory();
      pages_[cur_frame].is_dirty_ = false;
      pages_[cur_frame].pin_count_ = 0;

      // Allocate the page and assign this free frame the current page
      page_table_.insert({next_page_id_, cur_frame});
      pages_[cur_frame].page_id_ = next_page_id_;
      AllocatePage();

      // Pin the page
      pages_[cur_frame].pin_count_++;

      // Record the access time
      replacer_->RecordAccess(cur_frame);

      // return the current page
      *page_id = pages_[cur_frame].page_id_;
      return &pages_[cur_frame];
    }
  }

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  // if the page_id is present in the buffer pool return the corresponding page
  if (page_table_.count(page_id) > 0) {
    frame_id_t cur_frame = page_table_[page_id];

    // Record the access time and return current page
    replacer_->RecordAccess(cur_frame);
    return &pages_[cur_frame];
  } else {
    if (free_list_.size() > 0) {
      // get the first available free frame
      frame_id_t cur_frame = free_list_.front();
      free_list_.pop_front();

      // Allocate the page and assign this free frame the current page
      page_table_.insert({page_id, cur_frame});
      pages_[cur_frame].page_id_ = page_id;

      // read the data from disk if its already persisited
      if (page_id < next_page_id_) {
        auto promise1 = disk_scheduler_->CreatePromise();
        auto future1 = promise1.get_future();
        disk_scheduler_->Schedule(
            {/*is_write=*/false, pages_[cur_frame].data_, /*page_id=*/pages_[cur_frame].page_id_, std::move(promise1)});
        future1.get();
      }

      // Pin the page
      pages_[cur_frame].pin_count_++;

      // Record the access time
      replacer_->RecordAccess(cur_frame);

      // return the current page
      return &pages_[cur_frame];
    } else {
      // get the evicted frame from LKU Replacer
      frame_id_t cur_frame;

      if (!replacer_->Evict(&cur_frame)) {
        return nullptr;
      } else {
        // write this page to disk if its dirty
        if (pages_[cur_frame].is_dirty_) {
          auto promise1 = disk_scheduler_->CreatePromise();
          auto future1 = promise1.get_future();
          disk_scheduler_->Schedule({/*is_write=*/true, pages_[cur_frame].data_, /*page_id=*/pages_[cur_frame].page_id_,
                                     std::move(promise1)});
          future1.get();
        }

        // remove the current page id from the page table
        page_table_.erase(pages_[cur_frame].page_id_);

        // clear the contents of this page and reset the metadata
        pages_[cur_frame].ResetMemory();
        pages_[cur_frame].is_dirty_ = false;
        pages_[cur_frame].pin_count_ = 0;

        // Allocate the page and assign this free frame the current page
        page_table_.insert({page_id, cur_frame});
        pages_[cur_frame].page_id_ = page_id;

        // read the data from disk if its already persisited
        if (page_id < next_page_id_) {
          auto promise1 = disk_scheduler_->CreatePromise();
          auto future1 = promise1.get_future();
          disk_scheduler_->Schedule({/*is_write=*/false, pages_[cur_frame].data_,
                                     /*page_id=*/pages_[cur_frame].page_id_, std::move(promise1)});
          future1.get();
        }

        // Pin the page
        pages_[cur_frame].pin_count_++;

        // read this page from disk
        auto promise1 = disk_scheduler_->CreatePromise();
        auto future1 = promise1.get_future();
        disk_scheduler_->Schedule(
            {/*is_write=*/false, pages_[cur_frame].data_, /*page_id=*/pages_[cur_frame].page_id_, std::move(promise1)});
        future1.get();

        // Record the access time
        replacer_->RecordAccess(cur_frame);

        // return the current page
        return &pages_[cur_frame];
      }
    }
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if (page_table_.count(page_id) > 0) {
    frame_id_t cur_frame = page_table_[page_id];

    // return false if the pin value of this page is 0
    if (pages_[cur_frame].pin_count_ <= 0) {
      return false;
    } else {
      pages_[cur_frame].pin_count_--;

      // set the frame as evictable in replacer if the pin count reaches to 0
      if (pages_[cur_frame].pin_count_ == 0) {
        replacer_->SetEvictable(cur_frame, true);
      }
      // set the page as dirtly if the dirty flag is true
      if (is_dirty) {
        pages_[cur_frame].is_dirty_ = true;
      }

      return true;
    }
  }

  // if the page_id is not present in the buffer pool
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if (page_table_.count(page_id) > 0) {
    frame_id_t cur_frame = page_table_[page_id];

    // write the page to disk even if its not diry
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule(
        {/*is_write=*/true, pages_[cur_frame].data_, /*page_id=*/pages_[cur_frame].page_id_, std::move(promise1)});
    future1.get();

    pages_[cur_frame].is_dirty_ = false;

    return true;
  }

  // if the page_id is not present in the buffer pool
  return false;
}

void BufferPoolManager::FlushAllPages() {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  // flush all pages to disk
  for (auto &pair : page_table_) {
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule(
        {/*is_write=*/true, pages_[pair.second].data_, /*page_id=*/pages_[pair.second].page_id_, std::move(promise1)});
    future1.get();

    pages_[pair.second].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if (page_table_.count(page_id) > 0) {
    frame_id_t cur_frame = page_table_[page_id];

    // return false if the page is pinned
    if (pages_[cur_frame].pin_count_ > 0) {
      return false;
    }

    // delete the frame from replacer and delete this page from the page table and also add this frame into the free
    // frame list
    page_table_.erase(page_id);
    replacer_->Remove(cur_frame);
    free_list_.emplace_back(cur_frame);

    // clear the contents of this page and reset the metadata
    pages_[cur_frame].ResetMemory();
    pages_[cur_frame].is_dirty_ = false;
    pages_[cur_frame].pin_count_ = 0;

    // Deallocate page
    DeallocatePage(page_id);
  }

  // return true if the page_id to be deleted is not there in the buffer pool or return true when page is available too
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
