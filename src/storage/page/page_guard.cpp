#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // copy the contents(ownership) of the that pointer to this pointer
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  // make the contents of that pointer to null
  that.page_ = nullptr;
  that.is_dirty_ = false;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if(bpm_ != nullptr && page_ != nullptr) {
    // unpin this page
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

    // make the contents of that pointer to null
    page_ = nullptr;
    is_dirty_ = false;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
  if(this != &that) {
    //Drop the existing page
    Drop();

    // copy the contents(ownership) of the that pointer to this pointer
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;

    // make the contents of that pointer to null
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }

  return *this; 
}

BasicPageGuard::~BasicPageGuard(){
  if(bpm_ != nullptr && page_ != nullptr) {
    // unpin this page
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

    // make the contents of that pointer to null
    page_ = nullptr;
    is_dirty_ = false;
    bpm_ = nullptr;
  }
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { 
  // make an instance of ReadPageGuard by passing the correct parameters
  auto guarded_read_page = ReadPageGuard(bpm_, page_);
  
  // make the contents of that pointer to null
  page_ = nullptr;
  is_dirty_ = false;
  bpm_ = nullptr;

  return guarded_read_page; 
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { 
  // make an instance of WritePageGuard by passing the correct parameters
  auto guarded_write_page = WritePageGuard(bpm_, page_);
  
  // make the contents of that pointer to null
  page_ = nullptr;
  is_dirty_ = false;
  bpm_ = nullptr;
  
  return guarded_write_page; 
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
  // Get the read latch for this page
  page->RLatch();

  // copy the contents(ownership) of the that pointer to this pointer
  guard_.bpm_ = bpm;
  guard_.page_ = page;
  guard_.is_dirty_ = page->IsDirty();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // Move the latches from that pointer to this pointer
  guard_ = std::move(that.guard_);

  // copy the contents(ownership) of the that pointer to this pointer
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.page_ = that.guard_.page_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;

  // make the contents of that pointer to null
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
  that.guard_.bpm_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
  if(this != &that) {
    //Drop the existing page
    Drop();

    // Move the latches from that pointer to this pointer
    guard_ = std::move(that.guard_);

    // copy the contents(ownership) of the that pointer to this pointer
    this->guard_.bpm_ = that.guard_.bpm_;
    this->guard_.page_ = that.guard_.page_;
    this->guard_.is_dirty_ = that.guard_.is_dirty_;

    // make the contents of that pointer to null
    that.guard_.bpm_ = nullptr;
    that.guard_.page_ = nullptr;
    that.guard_.is_dirty_ = false;
  }

  return *this;   
}

void ReadPageGuard::Drop() {
  if(guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    // Release the Read latch
    guard_.page_->RUnlatch();

    // Drop this page
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {
  if(guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    // Release the Read latch
    guard_.page_->RUnlatch();

    // Drop this page
    guard_.Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
  // Get the read latch for this page
  page->WLatch();

  // copy the contents(ownership) of the that pointer to this pointer
  guard_.bpm_ = bpm;
  guard_.page_ = page;
  guard_.is_dirty_ = page->IsDirty();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // Move the latches from that pointer to this pointer
  guard_ = std::move(that.guard_);

  // copy the contents(ownership) of the that pointer to this pointer
  this->guard_.bpm_ = that.guard_.bpm_;
  this->guard_.page_ = that.guard_.page_;
  this->guard_.is_dirty_ = that.guard_.is_dirty_;

  // make the contents of that pointer to null
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
  that.guard_.bpm_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
  if(this != &that) {
    //Drop the existing page
    Drop();

    // Move the latches from that pointer to this pointer
    guard_ = std::move(that.guard_);

    // copy the contents(ownership) of the that pointer to this pointer
    this->guard_.bpm_ = that.guard_.bpm_;
    this->guard_.page_ = that.guard_.page_;
    this->guard_.is_dirty_ = that.guard_.is_dirty_;

    // make the contents of that pointer to null
    that.guard_.bpm_ = nullptr;
    that.guard_.page_ = nullptr;
    that.guard_.is_dirty_ = false;
  }

  return *this;
 }

void WritePageGuard::Drop() {
  if(guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    // Release the Read latch
    guard_.page_->WUnlatch();

    // Drop this page
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {
  if(guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    // Release the Read latch
    guard_.page_->WUnlatch();

    // Drop this page
    guard_.Drop();
  }
}  // NOLINT

}  // namespace bustub
