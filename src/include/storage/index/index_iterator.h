//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // 默认构造一个 end 迭代器
  IndexIterator();

  // 从给定叶子页和下标构造一个有效迭代器
  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_page_id, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd()const -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  // 相等比较运算符
  auto operator==(const IndexIterator &itr) const -> bool;

  // 不等比较运算符
  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // 当前迭代器所持有的缓冲池管理器指针
  BufferPoolManager *buffer_pool_manager_{nullptr};

  // 当前叶子页的 page_id
  page_id_t leaf_page_id_{INVALID_PAGE_ID};

  // 当前叶子页上的槽位下标
  int index_{0};

  // 当前叶子页对应的 Page*（保持 pinned）
  Page *page_{nullptr};

  // 标记该迭代器是否为 end 迭代器
  bool is_end_{true};
};

}  // namespace bustub