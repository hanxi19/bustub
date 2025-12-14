//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>  // 补充：B+Tree 中存放在页内的基础键值对类型，K 为索引键，V 为值（leaf 为 RID，internal 为 page_id）

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>  // 补充：后续所有 B+Tree 相关类统一使用的模板参数宏

// 补充：定义 B+Tree 页面的类型枚举（无效页 / 叶子页 / 内部页）
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * 补充：BPlusTreePage 是所有 B+Tree 结点（Internal / Leaf）的公共基类，只负责维护「页头（header）」的元信息。
 *
 * 其本质对应的是磁盘/内存中 page->data_ 这 4KB 数据的前 24 字节，布局如下：
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 *
 * - PageType      : 当前页是叶子页还是内部页（IndexPageType）
 * - LSN           : 日志序列号（用于恢复，Project4 才会用到，这里只需保存值）
 * - CurrentSize   : 当前页中存放的键值对数量（对 Internal/Leaf 的含义略有不同，但都是“元素个数”）
 * - MaxSize       : 当前页能够容纳的最大键值对数量（由页大小与键值对大小推算得到）
 * - ParentPageId  : 父结点在 B+Tree 中的 page_id（根节点为 INVALID_PAGE_ID）
 * - PageId        : 当前结点自身在 B+Tree 中的 page_id
 *
 * 注意：真正的键值对数组（MappingType array_[]）存放在派生类中（Internal / Leaf），这里不关心具体布局。
 */
class BPlusTreePage {
 public:
  // 补全：判断当前页是否为叶子页（通过 page_type_ == LEAF_PAGE）
  auto IsLeafPage() const -> bool;

  // 补全：判断当前页是否为根结点（通过 parent_page_id_ 是否为 INVALID_PAGE_ID）
  auto IsRootPage() const -> bool;

  // 补全：设置页类型（叶子 / 内部），通常在 Init 时由派生类调用
  void SetPageType(IndexPageType page_type);

  // 补全：获取当前页中实际存放的键值对个数（即 size_）
  auto GetSize() const -> int;

  // 补全：直接设置当前页中键值对个数（谨慎使用，一般由 Insert/Delete/分裂等逻辑维护）
  void SetSize(int size);

  // 补全：在当前 size_ 基础上增减指定数量（amount 可为负）
  void IncreaseSize(int amount);

  // 补全：获取页的最大容量（max_size_），即最多能容纳多少个键值对
  auto GetMaxSize() const -> int;

  // 补全：设置页的最大容量（通常在 Init 时由派生类根据模板参数计算后调用）
  void SetMaxSize(int max_size);

  // 补全：根据 max_size_ 计算出该页的最小合法占用（一般为 max_size_/2，用于判断是否需要合并/重分配）
  auto GetMinSize() const -> int;

  // 补全：获取父结点的 page_id（如果为根，则为 INVALID_PAGE_ID）
  auto GetParentPageId() const -> page_id_t;

  // 补全：设置父结点的 page_id（在分裂、合并、调整根节点时会频繁更新）
  void SetParentPageId(page_id_t parent_page_id);

  // 补全：获取当前结点自身的 page_id
  auto GetPageId() const -> page_id_t;

  // 补全：设置当前结点自身的 page_id（通常仅在 Init 时调用）
  void SetPageId(page_id_t page_id);

  // 补全：设置当前页的日志序列号（LSN），用于 WAL 日志恢复；Checkpoint1/2 中可以简单保存该值
  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // 补充：当前页类型（叶子 / 内部），用于在运行时判断如何 reinterpret_cast page->GetData()
  IndexPageType page_type_ __attribute__((__unused__));

  // 补充：日志序列号（Log Sequence Number），暂时在项目早期不会真正用到
  lsn_t lsn_ __attribute__((__unused__));

  // 补充：当前页中键值对的数量（对 Internal/Leaf 都是“元素个数”）
  int size_ __attribute__((__unused__));

  // 补充：页中键值对可容纳的最大数量（由页大小和 MappingType 大小计算）
  int max_size_ __attribute__((__unused__));

  // 补充：父页的 page_id（根结点为 INVALID_PAGE_ID）
  page_id_t parent_page_id_ __attribute__((__unused__));

  // 补充：当前页自身的 page_id（由 BufferPoolManager 分配）
  page_id_t page_id_ __attribute__((__unused__));
};

}  // namespace bustub
