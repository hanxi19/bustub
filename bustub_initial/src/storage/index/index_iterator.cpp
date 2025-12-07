/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：默认构造一个 end 迭代器（不持有任何页）
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
// 补全：析构时若仍持有 pinned 页面，需要在此处取消固定
INDEXITERATOR_TYPE::~IndexIterator() {  // NOLINT
  if (buffer_pool_manager_ != nullptr && page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
}

/**
 * 补充：从给定叶子页和下标构造一个有效迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_page_id, int index)
    : buffer_pool_manager_(buffer_pool_manager), leaf_page_id_(leaf_page_id), index_(index), page_(nullptr), is_end_(false) {
  if (buffer_pool_manager_ == nullptr || leaf_page_id_ == INVALID_PAGE_ID) {
    // 若传入非法参数，则退化为 end 迭代器
    buffer_pool_manager_ = nullptr;
    leaf_page_id_ = INVALID_PAGE_ID;
    index_ = 0;
    page_ = nullptr;
    is_end_ = true;
    return;
  }
  page_ = buffer_pool_manager_->FetchPage(leaf_page_id_);
  if (page_ == nullptr) {
    // Fetch 失败，同样退化为 end
    buffer_pool_manager_ = nullptr;
    leaf_page_id_ = INVALID_PAGE_ID;
    index_ = 0;
    is_end_ = true;
  }
}

INDEX_TEMPLATE_ARGUMENTS
// 补全：判断当前迭代器是否已经到达 end（无有效叶子页或被显式标记为 end）
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return is_end_ || leaf_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
// 补全：解引用迭代器，返回当前叶子页上 index_ 位置的键值对
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  assert(!IsEnd());
  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  assert(index_ >= 0 && index_ < leaf->GetSize());
  return leaf->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
// 补全：前置++，移动到下一个键值对；若越过最后一个元素则移动到下一叶子，必要时变为 end
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }

  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  index_++;

  if (index_ < leaf->GetSize()) {
    // 仍在当前叶子页范围内
    return *this;
  }

  // 当前叶子已耗尽，尝试跳到下一叶子
  page_id_t next_page_id = leaf->GetNextPageId();

  // 取消固定当前页
  buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
  page_ = nullptr;

  if (next_page_id == INVALID_PAGE_ID) {
    // 已经到达整棵树末尾，标记为 end
    leaf_page_id_ = INVALID_PAGE_ID;
    index_ = 0;
    is_end_ = true;
    return *this;
  }

  // 跳到下一叶子页
  leaf_page_id_ = next_page_id;
  page_ = buffer_pool_manager_->FetchPage(leaf_page_id_);
  auto *next_leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  index_ = 0;
  assert(next_leaf->GetSize() > 0);
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
// 补全：判断两个迭代器是否指向同一位置（叶子页 id 与 index 相同，或均为 end）
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (is_end_ && itr.is_end_) {
    return true;
  }
  return leaf_page_id_ == itr.leaf_page_id_ && index_ == itr.index_ && is_end_ == itr.is_end_;
}

INDEX_TEMPLATE_ARGUMENTS
// 补全：基于 operator== 的不等比较
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
