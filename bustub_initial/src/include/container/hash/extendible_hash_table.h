//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>
#include <vector>
#include <utility>

namespace bustub {

template <typename K, typename V>
class ExtendibleHashTable {
 private:
  // 嵌套桶类：存储KV对，维护局部深度和容量
  class Bucket {
    friend class ExtendibleHashTable<K, V>;  // 允许外部类访问私有成员
    using KVPair = std::pair<K, V>;
    std::list<KVPair> kv_pairs_;  // 存储KV对（链表便于插入删除）
    size_t max_size_;             // 桶的最大容量
    int local_depth_;             // 桶的局部深度

   public:
    using iterator = typename std::list<KVPair>::iterator;
    using const_iterator = typename std::list<KVPair>::const_iterator;

    Bucket(size_t max_size, int local_depth) : max_size_(max_size), local_depth_(local_depth) {}

    // 查找键：找到则赋值value并返回true
    bool Find(const K &key, V &value);
    // 删除键：找到则删除并返回true
    bool Remove(const K &key);
    // 插入键值对：存在则覆盖，满则返回false
    bool Insert(const K &key, const V &value);

    // 访问器
    int GetLocalDepth() const { return local_depth_; }
    void SetLocalDepth(int depth) { local_depth_ = depth; }
    size_t GetCurrentSize() const { return kv_pairs_.size(); }
    bool IsFull() const { return kv_pairs_.size() >= max_size_; }

    // 迭代器接口（用于拆分时重新分配KV对）
    iterator begin() { return kv_pairs_.begin(); }
    iterator end() { return kv_pairs_.end(); }
  };

  // 哈希表核心成员
  std::vector<Bucket *> dir_;          // 目录：存储桶指针
  int global_depth_;                   // 全局深度
  size_t bucket_size_;                 // 每个桶的最大容量
  int num_buckets_;                    // 桶的总数量（去重后）
  mutable std::mutex latch_;           // 互斥锁（保证线程安全）

  // 辅助函数：拆分桶
  void SplitBucket(Bucket *bucket);
  // 计算键对应的目录索引（已在cpp中实现）
  size_t IndexOf(const K &key);

 public:
  // 构造函数
  explicit ExtendibleHashTable(size_t bucket_size);
  // 析构函数（释放桶内存）
  ~ExtendibleHashTable();

  // 核心操作
  bool Find(const K &key, V &value);
  bool Remove(const K &key);
  void Insert(const K &key, const V &value);

  // 元数据查询（已在cpp中实现加锁）
  int GetGlobalDepth() const;
  int GetLocalDepth(int dir_index) const;
  int GetNumBuckets() const;

 private:
  // 内部元数据查询（无锁，供加锁函数调用）
  int GetGlobalDepthInternal() const;
  int GetLocalDepthInternal(int dir_index) const;
  int GetNumBucketsInternal() const;
};

}  // namespace bustub