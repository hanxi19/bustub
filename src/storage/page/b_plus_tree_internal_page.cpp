//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：初始化内部页的基础元数据（页类型、页号、父页号、当前大小和最大容量）
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // 补全：返回给定下标对应的 key（存储在 flexible array 的 first 中）
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
// 补全：将给定下标位置的 key 更新为传入的 key
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetMaxSize());
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：返回给定下标对应的 value（即子节点的 page_id，存储在 second 中）
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
// 补充：设置指定下标位置的 child page_id（Value）
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(index >= 0 && index < GetMaxSize());
  array_[index].second = value;
}

/**
 * 补充：查找给定 child page_id 在当前内部页中的下标
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  // 补充：线性扫描 array_，返回第一个 second 等于 value 的下标，找不到则返回 -1
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/**
 * 补充：根据 key 在当前内部页中执行二分查找，返回应该访问的 child page_id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // 查找满足 KeyAt(i) <= key 的最大 i（i >= 1），特殊情况 i=0 表示最左 child
  int left = 1;
  int right = GetSize() - 1;
  int result_index = 0;  // 默认走最左 child
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) <= 0) {
      result_index = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return ValueAt(result_index);
}

/**
 * 补充：在 old_value 之后插入 (new_key, new_value) 结点，返回插入后的 size
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  int index = ValueIndex(old_value);
  assert(index >= 0);
  int insert_index = index + 1;
  // 整体右移，为新结点腾出空间
  for (int i = GetSize(); i > insert_index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[insert_index].first = new_key;
  array_[insert_index].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/**
 * 补充：删除给定下标处的 (key, value) 对，后面元素左移
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * 补充：用于根节点收缩时，只剩一个孩子，返回该唯一 child 的 page_id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  assert(GetSize() == 1);
  ValueType child = ValueAt(0);
  SetSize(0);
  return child;
}

/**
 * 补充：在分裂时，将当前页后一半元素移动到 recipient 内部页，并更新子节点父指针
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int total = GetSize();
  int move_count = total / 2;
  int start_index = total - move_count;
  recipient->CopyNFrom(array_ + start_index, move_count, buffer_pool_manager);
  SetSize(total - move_count);
}

/**
 * 补充：在合并时，将当前页所有元素移动到 recipient 内部页（parent_index 为父中的分隔键下标）
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, int parent_index,
                                               BufferPoolManager *buffer_pool_manager) {
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
  (void)parent_index;
}

/**
 * 补充：在重分配时，将当前页的第一个元素移动到 recipient 的末尾，并用 middle_key 更新移动元素的 key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType item{middle_key, array_[0].second};
  recipient->CopyNFrom(&item, 1, buffer_pool_manager);
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * 补充：在重分配时，将当前页的最后一个元素移动到 recipient 的开头，并用 middle_key 更新 recipient 中对应 key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType last = array_[GetSize() - 1];
  IncreaseSize(-1);
  // 将 recipient 现有元素整体右移一位
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0].first = middle_key;
  recipient->array_[0].second = last.second;
  recipient->IncreaseSize(1);
  (void)buffer_pool_manager;
}

/**
 * 补充：将一段连续的 (key, value) 对从 src 拷贝到当前内部页末尾，并更新其子节点的父指针
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size,
                                               BufferPoolManager *buffer_pool_manager) {
  int cur_size = GetSize();
  for (int i = 0; i < size; i++) {
    array_[cur_size + i] = items[i];
    auto *page = buffer_pool_manager->FetchPage(array_[cur_size + i].second);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(node->GetPageId(), true);
  }
  IncreaseSize(size);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
