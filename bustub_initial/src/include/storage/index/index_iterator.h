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
  // you may define your own constructor based on your member variables
  // 补全：默认构造一个 end 迭代器
  IndexIterator();

  // 补充：从给定叶子页和下标构造一个有效迭代器
  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_page_id, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  // 补全：声明相等比较运算符（具体实现见 cpp）
  auto operator==(const IndexIterator &itr) const -> bool;

  // 补全：声明不等比较运算符（具体实现见 cpp）
  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // 补充：当前迭代器所持有的缓冲池管理器指针
  BufferPoolManager *buffer_pool_manager_{nullptr};

  // 补充：当前叶子页的 page_id，用于判断两个迭代器是否指向同一位置
  page_id_t leaf_page_id_{INVALID_PAGE_ID};

  // 补充：当前叶子页上的槽位下标
  int index_{0};

  // 补充：当前叶子页对应的 Page*（保持 pinned），在 ++ 时会切换到下一个叶子
  Page *page_{nullptr};

  // 补充：标记该迭代器是否为 end 迭代器
  bool is_end_{true};
};

}  // namespace bustub
