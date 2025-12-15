//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：初始化叶子页的基础元数据（页类型、页号、父页号、next 指针、当前大小和最大容量）
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：返回链表中下一个叶子页的 page_id
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
// 补全：设置链表中下一个叶子页的 page_id
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // 补全：返回给定下标对应的 key（存储在 flexible array 的 first 中）
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

/**
 * 补充：返回给定下标处的键值对 MappingType 引用
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> const MappingType & {
  // 补充：直接返回 array_[index]，用于遍历和调试
  assert(index >= 0 && index < GetSize());
  return array_[index];
}

/**
 * 补充：在当前叶子页中查找第一个 >= key 的位置（用于插入位置或 Begin(key)）
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

/**
 * 补充：在叶子页中查找给定 key 对应的 value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const -> bool {
  int index = KeyIndex(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    value = array_[index].second;
    return true;
  }
  return false;
}

/**
 * 补充：按有序位置插入 (key, value)，返回插入后的 size
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                        const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);
  // 将 index 之后的元素整体右移一位
  for (int i = GetSize(); i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  return GetSize();
}

/**
 * 补充：按 key 删除记录，返回删除后的 size（若 key 不存在则 size 不变）
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                       const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);
  if (index == GetSize() || comparator(array_[index].first, key) != 0) {
    // 未找到要删除的 key
    return GetSize();
  }
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/**
 * 补充：在分裂时，将当前页后一半元素移动到 recipient 叶子页
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int total = GetSize();
  int move_count = total / 2;
  int start_index = total - move_count;
  // 将 [start_index, total) 的元素复制到 recipient 末尾
  for (int i = 0; i < move_count; i++) {
    recipient->array_[recipient->GetSize() + i] = array_[start_index + i];
  }
  recipient->IncreaseSize(move_count);
  IncreaseSize(-move_count);
}

/**
 * 补充：在合并时，将当前页所有元素移动到 recipient 叶子页末尾
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int cur_size = GetSize();
  for (int i = 0; i < cur_size; i++) {
    recipient->array_[recipient->GetSize() + i] = array_[i];
  }
  recipient->IncreaseSize(cur_size);
  SetSize(0);
}

/**
 * 补充：在重分配时，将当前页的第一个元素移动到 recipient 的末尾
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // 将自己的第一个元素追加到 recipient 末尾
  recipient->array_[recipient->GetSize()] = array_[0];
  recipient->IncreaseSize(1);
  // 自身所有元素左移一位
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * 补充：在重分配时，将当前页的最后一个元素移动到 recipient 的开头
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  // 将 recipient 现有元素整体右移一位，为新元素腾出第 0 个位置
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  // 把当前页最后一个元素放到 recipient 开头
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
