//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;

  // 补充：返回指定下标处的键值对引用，便于在 B+Tree 中遍历或调试
  auto GetItem(int index) const -> const MappingType &;

  // 补充：在当前叶子页中查找 key，返回第一个 >= key 的下标（用于插入位置或 Begin(key)）
  auto KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int;

  // 补充：在叶子页中查找给定 key 对应的 value，成功返回 true 并写入 value
  auto Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const -> bool;

  // 补充：按有序位置插入 (key, value)，返回插入后的 size
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int;

  // 补充：按 key 删除记录，返回删除后的 size（若 key 不存在则 size 不变）
  auto RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int;

  // 补充：在分裂时，将当前页后一半元素移动到 recipient 叶子页
  void MoveHalfTo(BPlusTreeLeafPage *recipient);

  // 补充：在合并时，将当前页所有元素移动到 recipient 叶子页末尾
  void MoveAllTo(BPlusTreeLeafPage *recipient);

  // 补充：在重分配时，将当前页的第一个元素移动到 recipient 的末尾
  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);

  // 补充：在重分配时，将当前页的最后一个元素移动到 recipient 的开头
  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
