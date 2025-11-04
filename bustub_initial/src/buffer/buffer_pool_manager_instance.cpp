//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // Find a free frame
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // Use free list first
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    // Evict a page if no free frames
    Page *victim_page = &pages_[frame_id];
    
    // If the victim page is dirty, write it back to disk
    if (victim_page->IsDirty()) {
      disk_manager_->WritePage(victim_page->GetPageId(), victim_page->GetData());
      victim_page->is_dirty_ = false;
    }
    
    // Remove from page table
    page_table_->Remove(victim_page->GetPageId());
  } else {
    // No available frames
    return nullptr;
  }

  // Allocate a new page id
  *page_id = AllocatePage();

  // Get the page and initialize it
  Page *page = &pages_[frame_id];
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  // Add to page table
  page_table_->Insert(*page_id, frame_id);

  // Record access and set as non-evictable
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // Check if page is already in buffer pool
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    
    // Record access and set as non-evictable
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    
    return page;
  }

  // Page not in buffer pool, need to fetch from disk
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    // Use free list first
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&new_frame_id)) {
    // Evict a page if no free frames
    Page *victim_page = &pages_[new_frame_id];
    
    // If the victim page is dirty, write it back to disk
    if (victim_page->IsDirty()) {
      disk_manager_->WritePage(victim_page->GetPageId(), victim_page->GetData());
      victim_page->is_dirty_ = false;
    }
    
    // Remove from page table
    page_table_->Remove(victim_page->GetPageId());
  } else {
    // No available frames
    return nullptr;
  }

  // Read page from disk
  Page *page = &pages_[new_frame_id];
  disk_manager_->ReadPage(page_id, page->GetData());
  
  // Initialize page metadata
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  // Add to page table
  page_table_->Insert(page_id, new_frame_id);

  // Record access and set as non-evictable
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;  // Page not found
  }

  Page *page = &pages_[frame_id];
  if (page->pin_count_ <= 0) {
    return false;  // Pin count already 0
  }

  page->pin_count_--;

  // Update dirty flag
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  // If pin count reaches 0, set as evictable
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;  // Page not found
  }

  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // Page not in buffer pool
    DeallocatePage(page_id);
    return true;
  }

  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    // Page is pinned, cannot delete
    return false;
  }

  // If page is dirty, write it back to disk
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }

  // Remove from page table and replacer
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  // Reset page metadata
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();

  // Add frame back to free list
  free_list_.push_back(frame_id);

  // Deallocate page id
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub