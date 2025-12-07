//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  // 补充：设置指定下标的 child page_id（Value）
  void SetValueAt(int index, const ValueType &value);

  // 补充：查找给定 child page_id 在当前内部页中的下标，用于在父结点中定位孩子
  auto ValueIndex(const ValueType &value) const -> int;

  // 补充：根据 key 在当前内部页中执行二分查找，返回应该访问的 child page_id
  auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  // 补充：在 old_value 之后插入 (new_key, new_value) 结点，返回插入后的 size
  auto InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) -> int;

  // 补充：删除给定下标处的 (key, value) 对，后面元素左移
  void Remove(int index);

  // 补充：用于根节点收缩时，只剩一个孩子，返回该唯一 child 的 page_id
  auto RemoveAndReturnOnlyChild() -> ValueType;

  // 补充：在分裂时，将当前页后一半元素移动到 recipient 内部页，并更新子节点父指针
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  // 补充：在合并时，将当前页所有元素移动到 recipient 内部页（parentIndex 为父中的分隔键下标）
  void MoveAllTo(BPlusTreeInternalPage *recipient, int parent_index, BufferPoolManager *buffer_pool_manager);

  // 补充：在重分配时，将当前页的第一个元素移动到 recipient 的末尾，并用 middle_key 更新移动元素的 key
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                        BufferPoolManager *buffer_pool_manager);

  // 补充：在重分配时，将当前页的最后一个元素移动到 recipient 的开头，并用 middle_key 更新 recipient 中对应 key
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                         BufferPoolManager *buffer_pool_manager);

  // 补充：将一段连续的 (key, value) 对从 src 拷贝到当前内部页末尾（仅供 MoveHalfTo/MoveAllTo 使用）
  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
