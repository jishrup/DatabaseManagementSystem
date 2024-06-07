//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t frame_id, size_t k) {
  fid_ = frame_id;
  k_ = k;
  cur_size_ = 0;
}

void LRUKNode::AddAcceesTime() {

  // Get the current time from the high-resolution clock
  auto now = std::chrono::high_resolution_clock::now();

  // Convert the time point to a duration since epoch
  auto duration = now.time_since_epoch();

  // Convert the duration to microseconds
  auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  // Convert to size_t
  size_t currentTime = static_cast<size_t>(microseconds);

  // insert the current time into the history list
  history_.push_back(currentTime);

  // increase the size of the history list else pop the first element
  if(cur_size_<k_) {
    cur_size_++;
  } else {
    history_.pop_front();
  }
}

inline void LRUKNode::ClearAcceesTime() {
  history_.clear();
  cur_size_ = 0;
}

inline void LRUKNode::SetEvictableStatus(bool set_evictable) {
  is_evictable_=set_evictable;
}

inline size_t LRUKNode::GetLatestAccessTime() const{
  if(cur_size_==0)
    return 0;
  else
    return history_.front();
}

inline size_t LRUKNode::GetNumAceessTime() const{
  return cur_size_;
}

inline bool LRUKNode::IsFrameEvictable() const {
  return is_evictable_;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto &pair : node_store_) {
    delete pair.second;
  }
  node_store_.clear(); 
  replacer_.clear(); // can be removed
  inf_replacer_.clear(); // can be removed
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if(curr_size_==0) {
    *frame_id = -1;
    return false; 
  } else {
    size_t curtime = std::numeric_limits<size_t>::max();
    frame_id_t curframe = -1;
    LRUKNode * remnode = NULL;

    if(inf_replacer_.size() > 0) {
      for(const frame_id_t &fid : inf_replacer_) {
        LRUKNode * cur_node = node_store_[fid];

        if(cur_node->GetLatestAccessTime() < curtime) {
          curtime = cur_node->GetLatestAccessTime();
          curframe = fid;
          remnode = cur_node;
        }
      }
    } else {
      for(const frame_id_t &fid : replacer_) {
        LRUKNode * cur_node = node_store_[fid];

        if(cur_node->GetLatestAccessTime() < curtime) {
          curtime = cur_node->GetLatestAccessTime();
          curframe = fid;
          remnode = cur_node;
        }
      }
    }

    if(remnode->GetNumAceessTime() < k_){
      auto index = inf_replacer_.find(curframe);
      inf_replacer_.erase(index);
    }

    auto index = replacer_.find(curframe);
    replacer_.erase(index);
    curr_size_--;

    remnode->ClearAcceesTime();
    remnode->SetEvictableStatus(false);
    *frame_id = curframe;

    return true;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // abort the process if frameid is invalid, i.e greater than the replacer size
  BUSTUB_ASSERT((size_t)frame_id<replacer_size_, "frameid invalid, greater than the replacer size");

  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if(node_store_.count(frame_id)>0) {
    LRUKNode * cur_node = node_store_[frame_id];

    if(cur_node->IsFrameEvictable() && cur_node->GetNumAceessTime() == k_-1) {
      auto index = inf_replacer_.find(frame_id);
      inf_replacer_.erase(index);
    }
    cur_node->AddAcceesTime();
  } else {
    LRUKNode * cur_node = new LRUKNode(frame_id,k_);
    
    cur_node->AddAcceesTime();
    node_store_.insert({frame_id,cur_node});
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // abort the process if frameid is invalid, i.e greater than the replacer size
  BUSTUB_ASSERT((size_t)frame_id<replacer_size_, "frameid invalid, greater than the replacer size");

  // abort the process if frameid to set as evicatable or not is not accessed by the replacer before
  BUSTUB_ASSERT(node_store_.count(frame_id) != 0, "frameid is not accessed before");

  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if(set_evictable) {
    if(replacer_.count(frame_id) == 0) {
      replacer_.insert(frame_id);
      LRUKNode * cur_node = node_store_[frame_id];

      if(cur_node->GetNumAceessTime() < k_)
        inf_replacer_.insert(frame_id); 

      cur_node->SetEvictableStatus(true);
      curr_size_++;
    } else {
      // abort the process if frameid is already present in the replacer
      BUSTUB_ASSERT(true, "frameid already present in the replacer, again setting the same frameid as evictable.");
    }
  } 

  if(!set_evictable) {
    if(replacer_.count(frame_id) > 0) {
      LRUKNode * cur_node = node_store_[frame_id];

      auto index = replacer_.find(frame_id);
      replacer_.erase(index);

      if(cur_node->GetNumAceessTime() < k_) {
        index = inf_replacer_.find(frame_id);
        inf_replacer_.erase(index);
      }

      cur_node->SetEvictableStatus(false);
      curr_size_--;
    } else {
      // abort the process if frameid which is supposed to be set as not evictable is not already evictable.
      BUSTUB_ASSERT(true, "frameid which is supposed to be set as not evictable is not already evictable.");
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // abort the process if frameid is invalid, i.e greater than the replacer size
  BUSTUB_ASSERT((size_t)frame_id<replacer_size_, "frameid invalid, greater than the replacer size");

  // Take the mutex latch for the record operation
  std::unique_lock<std::mutex> lk(latch_);

  if(replacer_.count(frame_id)>0) {
    LRUKNode * cur_node = node_store_[frame_id];

    curr_size_--;

    auto index = replacer_.find(frame_id);
    replacer_.erase(index);

    index = inf_replacer_.find(frame_id);
    if(*index > 0)
      inf_replacer_.erase(index);

    cur_node->ClearAcceesTime();
    cur_node->SetEvictableStatus(false);
  }
}

auto LRUKReplacer::Size() -> size_t { 
  return curr_size_; 
}

}  // namespace bustub
