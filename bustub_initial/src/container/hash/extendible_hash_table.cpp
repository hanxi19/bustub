//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include <unordered_set>  // 新增：添加std::unordered_set所需头文件
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

// ------------- ExtendibleHashTable 构造与析构 -------------
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  global_depth_ = 0;
  num_buckets_ = 1;
  // 初始目录大小为1（2^0），指向一个空桶
  dir_.emplace_back(new Bucket(bucket_size_, global_depth_));
}

template <typename K, typename V>
ExtendibleHashTable<K, V>::~ExtendibleHashTable() {
  // 去重释放桶（避免目录中重复指针导致double free）
  std::unordered_set<Bucket *> unique_buckets;
  for (Bucket *bucket : dir_) {
    unique_buckets.insert(bucket);
  }
  for (Bucket *bucket : unique_buckets) {
    delete bucket;
  }
}

// ------------- Bucket 类实现 -------------
template <typename K, typename V>
bool ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) {
  for (const auto &pair : kv_pairs_) {
    if (pair.first == key) {
      value = pair.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
bool ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) {
  for (auto it = kv_pairs_.begin(); it != kv_pairs_.end(); ++it) {
    if (it->first == key) {
      kv_pairs_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
bool ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) {
  // 1. 检查键是否已存在，存在则覆盖
  for (auto &pair : kv_pairs_) {
    if (pair.first == key) {
      pair.second = value;
      return true;
    }
  }
  // 2. 桶满则返回false（需拆分）
  if (IsFull()) {
    return false;
  }
  // 3. 插入新KV对
  kv_pairs_.emplace_back(key, value);
  return true;
}

// ------------- 辅助函数：拆分桶 -------------
template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(Bucket *bucket) {
  // 步骤1：若局部深度=全局深度，先扩展目录
  if (bucket->GetLocalDepth() == GetGlobalDepthInternal()) {
    size_t old_dir_size = dir_.size();
    dir_.resize(old_dir_size * 2);  // 目录大小翻倍
    // 新目录项复制原目录对应指针（后半部分指向前半部分桶）
    for (size_t i = 0; i < old_dir_size; ++i) {
      dir_[i + old_dir_size] = dir_[i];
    }
    global_depth_++;  // 全局深度加1
  }

  // 步骤2：增加原桶的局部深度
  int new_local_depth = bucket->GetLocalDepth() + 1;
  bucket->SetLocalDepth(new_local_depth);

  // 步骤3：创建新桶（局部深度与原桶相同）
  auto new_bucket = new Bucket(bucket_size_, new_local_depth);
  num_buckets_++;

  // 步骤4：分组目录指针（按新局部深度的最高位拆分）
  int split_bit = new_local_depth - 1;  // 区分原桶和新桶的位（0-based）
  size_t split_mask = 1 << split_bit;   // 用于检查split_bit位是否为1

  for (size_t i = 0; i < dir_.size(); ++i) {
    if (dir_[i] == bucket) {
      // split_bit位为1的目录项指向新桶，为0的保留原桶
      if ((i & split_mask) != 0) {
        dir_[i] = new_bucket;
      }
    }
  }

  // 步骤5：重新分配原桶的KV对到原桶和新桶
  auto it = bucket->begin();
  while (it != bucket->end()) {
    const K &key = it->first;
    size_t target_index = IndexOf(key);  // 计算KV对应的目标目录索引
    if (dir_[target_index] == new_bucket) {
      // 移到新桶
      new_bucket->Insert(key, it->second);
      it = bucket->kv_pairs_.erase(it);  // 从原桶删除
    } else {
      ++it;
    }
  }
}

// ------------- 核心操作：Find/Remove/Insert -------------
template <typename K, typename V>
size_t ExtendibleHashTable<K, V>::IndexOf(const K &key) {
  int mask = (1 << global_depth_) - 1;  // 取哈希值的低global_depth位
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
bool ExtendibleHashTable<K, V>::Find(const K &key, V &value) {
  std::scoped_lock lock(latch_);  // 加锁保证线程安全
  size_t index = IndexOf(key);
  if (index >= dir_.size()) {
    return false;  // 索引超出目录范围（理论上不会发生）
  }
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
bool ExtendibleHashTable<K, V>::Remove(const K &key) {
  std::scoped_lock lock(latch_);
  size_t index = IndexOf(key);
  if (index >= dir_.size()) {
    return false;
  }
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock lock(latch_);
  while (true) {
    size_t index = IndexOf(key);
    Bucket *target_bucket = dir_[index];
    // 尝试插入：成功则退出循环，失败则拆分后重试
    if (target_bucket->Insert(key, value)) {
      break;
    }
    SplitBucket(target_bucket);
  }
}

// ------------- 元数据查询（加锁） -------------
template <typename K, typename V>
int ExtendibleHashTable<K, V>::GetGlobalDepth() const {
  std::scoped_lock lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
int ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const {
  std::scoped_lock lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
int ExtendibleHashTable<K, V>::GetNumBuckets() const {
  std::scoped_lock lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
int ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const {
  return global_depth_;
}

template <typename K, typename V>
int ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const {
  assert(dir_index >= 0 && static_cast<size_t>(dir_index) < dir_.size());
  return dir_[dir_index]->GetLocalDepth();
}

template <typename K, typename V>
int ExtendibleHashTable<K, V>::GetNumBucketsInternal() const {
  return num_buckets_;
}

// ------------- 模板实例化（供测试和实际使用） -------------
template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub