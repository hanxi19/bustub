/**
* index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {  // NOLINT
  if (buffer_pool_manager_ != nullptr && page_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = nullptr;  // 避免重复释放
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_page_id, int index)
    : buffer_pool_manager_(buffer_pool_manager), leaf_page_id_(leaf_page_id), index_(index), page_(nullptr), is_end_(false) {
  if (buffer_pool_manager_ == nullptr || leaf_page_id_ == INVALID_PAGE_ID) {
    is_end_ = true;
    return;
  }
  page_ = buffer_pool_manager_->FetchPage(leaf_page_id_);
  if (page_ == nullptr) {
    is_end_ = true;
    return;
  }
  page_->RLatch();

  // 初始化时检查index是否越界
  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  if (index_ < 0 || index_ >= leaf->GetSize()) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
    page_ = nullptr;
    is_end_ = true;
    leaf_page_id_ = INVALID_PAGE_ID;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd()const -> bool {
  return is_end_ || leaf_page_id_ == INVALID_PAGE_ID || page_ == nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(!IsEnd());
  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  assert(index_ >= 0 && index_ < leaf->GetSize());
  return leaf->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }

  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  index_++;

  // 检查当前页是否还有元素
  if (index_ < leaf->GetSize()) {
    return *this;
  }

  // 切换到下一页
  page_id_t next_page_id = leaf->GetNextPageId();

  // 释放当前页
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
  page_ = nullptr;
  leaf_page_id_ = INVALID_PAGE_ID;

  if (next_page_id == INVALID_PAGE_ID) {
    // 确实没有下一页，标记为end
    is_end_ = true;
    index_ = 0;
    return *this;
  }

  // 加载下一页
  leaf_page_id_ = next_page_id;
  page_ = buffer_pool_manager_->FetchPage(leaf_page_id_);
  if (page_ == nullptr) {
    is_end_ = true;
    return *this;
  }
  page_->RLatch();

  auto *next_leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  // 下一页必须有元素，否则视为end（避免空页导致的无效遍历）
  if (next_leaf->GetSize() == 0) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
    page_ = nullptr;
    leaf_page_id_ = INVALID_PAGE_ID;
    is_end_ = true;
    index_ = 0;
    return *this;
  }

  index_ = 0;
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (IsEnd() && itr.IsEnd()) {
    return true;
  }
  if (IsEnd() || itr.IsEnd()) {
    return false;
  }
  return leaf_page_id_ == itr.leaf_page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !(*this == itr);
}

// 模板实例化
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub