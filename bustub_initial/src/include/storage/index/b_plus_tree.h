//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  // 补充：从根节点开始查找叶子页
  // left_most=true 时一路走到最左叶子（用于 Begin），否则按照给定 key 做查找
  auto FindLeafPage(const KeyType &key, bool left_most, Transaction *transaction) -> LeafPage *;

  // 补充：当前树为空时，创建一棵只包含一个叶子页的新树，并插入第一条 (key, value)
  void StartNewTree(const KeyType &key, const ValueType &value);

  // 补充：在非空 B+Tree 中，将 (key, value) 插入合适的叶子页；若 key 已存在返回 false
  auto InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool;

  // 补充：当节点已满时，将其分裂为两个节点，返回新分裂出来的节点指针
  template <typename N>
  auto Split(N *node) -> N *;

  // 补充：在 old_node 分裂出 new_node 后，把分裂键插入父节点；必要时父节点也可能继续分裂
  template <typename N>
  void InsertIntoParent(N *old_node, const KeyType &key, N *new_node, Transaction *transaction);

  // 补充：删除后若节点小于最小容量，尝试与兄弟结点合并或重分配，返回是否树结构发生了变化
  template <typename N>
  auto CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool;

  // 补充：将 node 与 neighbor_node 进行真正的合并（由 parent 和 index 决定左右关系）
  template <typename N>
  auto Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index, Transaction *transaction) -> bool;

  // 补充：在不合并的情况下，从 neighbor_node 借一个键值对重分配给 node
  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  // 补充：在删除导致根节点过小或为空时，调整 root_page_id_（可能降低树高）
  auto AdjustRoot(BPlusTreePage *old_root_node) -> bool;

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
