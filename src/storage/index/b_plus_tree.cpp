#include <string>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <cassert>
#include <utility>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {



INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      root_latch_() {}

INDEX_TEMPLATE_ARGUMENTS
// 释放事务中持有的所有页面的写锁并 Unpin 它们。
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(BufferPoolManager *bpm, Transaction *txn) {
  if (txn == nullptr) {
    return;
  }
  auto page_set = txn->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page != nullptr) {
      page->WUnlatch();  // 释放页面写锁
      // 【关键修复】: 必须 Unpin，因为它在 FindLeafPage 中被保留了 Pin 计数。
      //问题可能与直接使用类中的成员有关
      bpm->UnpinPage(page->GetPageId(), false);
    }
  }
}
/** 检查 B+ 树是否为空 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * 执行点查询：返回与给定键关联的唯一值。
 * 使用 Read Latch-Coupling 策略。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // FindLeafPage 负责加锁和 Latch-Coupling
  auto *page = FindLeafPage(key, false, Operation::READ, transaction);
  if (page == nullptr) {
    return false;
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool found = leaf->Lookup(key, value, comparator_);

  // 释放当前叶节点的读锁并 Unpin
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);

  if (found) {
    result->clear();
    result->push_back(value);
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * 插入键值对。
 * 使用 Write Latch-Coupling 策略，并在安全时释放祖先锁。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 1. 处理空树（加 root_latch_ 保护 root_page_id_）
  /*
   * 通过 C++ 的 RAII (Resource Acquisition Is Initialization) 机制，
   * 即通过 std::lock_guard 对象的生命周期结束来释放root_latch_
   */
  {
    std::lock_guard<std::mutex> lock(root_latch_);
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }

  }

  // 2. 查找叶节点并沿途加写锁 (WLatch)
  auto *page = FindLeafPage(key, false, Operation::INSERT, transaction);
  if (page == nullptr) {
    return false;
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

  // 3. 检查重复键
  ValueType temp_val;
  if (leaf->Lookup(key, temp_val, comparator_)) {
    // 发现重复键，释放祖先锁和当前叶节点的锁（每个键只能对应一个值，重复键插入必须失败）
    ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }

  // 4. 插入记录
  leaf->Insert(key, value, comparator_);

  // 5. 检查是否需要分裂 (安全判断)
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    // 安全：不需要分裂，释放祖先锁
    ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return true;
  }

  // 6. 不安全：执行分裂并插入到父节点
  auto *new_leaf = Split<LeafPage>(leaf);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());

  KeyType split_key = new_leaf->KeyAt(0);
  InsertIntoParent(leaf, split_key, new_leaf, transaction);

  // 7. 释放叶节点及其分裂页的锁
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);

  // 8. 释放所有祖先锁（InsertIntoParent 会在必要时保留父节点的锁）
  ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * 删除键值对。
 * 使用 Write Latch-Coupling 策略，并在安全时释放祖先锁。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  // 1. 查找叶节点并沿途加写锁
  auto *page = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (page == nullptr) {
    return;
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

  // 2. 尝试删除记录
  int old_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, comparator_);

  // 3. 检查是否删除成功
  if (new_size == old_size) {
    // 未找到键，释放锁
    ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }

  // 4. 检查是否需要合并或重新分配 (安全判断)
  if (leaf->IsRootPage() || new_size >= leaf->GetMinSize()) {
    // 安全：是根节点或未达到最小大小，释放祖先锁
    ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }

  // 5. 不安全：执行合并或重新分配
  CoalesceOrRedistribute(leaf, transaction);

  // 6. 释放当前叶节点的锁
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);

  // 7. 释放所有祖先锁（CoalesceOrRedistribute 会在必要时保留父节点的锁）
  ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * 返回指向最左侧叶节点的迭代器（Begin）。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  // 使用 left_most=true 查找最左叶节点。使用 Read Latch-Coupling
  KeyType dummy_key{};
  auto *page = FindLeafPage(dummy_key, true, Operation::READ, nullptr);
  if (page == nullptr) {
    return End();
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf->GetSize() == 0) {
    // 空叶节点，返回 End
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return End();
  }

  page_id_t leaf_page_id = leaf->GetPageId();
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);

  // 构造迭代器，指向第一个元素
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, 0);
}

/**
 * 返回指向第一个大于或等于给定键的元素的迭代器（Begin(key)）。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  // 查找包含 key 的叶节点。使用 Read Latch-Coupling
  auto *page = FindLeafPage(key, false, Operation::READ, nullptr);
  if (page == nullptr) {
    return End();
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  // 查找第一个大于或等于 key 的索引
  int index = leaf->KeyIndex(key, comparator_);

  // 关键修复：KeyIndex 返回第一个大于或等于 key 的索引。
  // 如果 index > 0，且 index-1 处的键等于 key，说明存在重复键，需要退回
  // 到第一个等于 key 的位置，以支持 Range Scan。
  if (index > 0 && comparator_(leaf->KeyAt(index - 1), key) == 0) {
    index--;
  }

  page_id_t leaf_page_id = leaf->GetPageId();

  if (index >= leaf->GetSize()) {
    // 目标键在当前页不存在，且大于所有键，需要检查下一页
    page_id_t next_page_id = leaf->GetNextPageId();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);

    if (next_page_id == INVALID_PAGE_ID) {
      return End();
    }
    // 指向下一页的第一个元素
    return INDEXITERATOR_TYPE(buffer_pool_manager_, next_page_id, 0);
  }

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  // 指向当前页的 index 元素
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, index);
}

/**
 * 返回 end 迭代器。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
}

/** 返回根页 ID */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/** 更新 Header Page 中的根页 ID */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*****************************************************************************
 * HELPER FUNCTIONS (FIND / INSERT / SPLIT / DELETE)
 *****************************************************************************/

/**
 * 查找目标叶节点（并发核心逻辑）。
 * 实现了 Latch-Coupling 策略。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool left_most, Operation op, Transaction *transaction) -> Page * {
  // 1. 锁根节点保护 root_page_id_
  root_latch_.lock();

  if (IsEmpty()) {
    root_latch_.unlock();
    return nullptr;
  }

  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    root_latch_.unlock();
    return nullptr;
  }

  // 2. 加当前页面的锁（读/写）
  if (op == Operation::READ) {
    page->RLatch();
  } else {
    page->WLatch();
  }

  // 3. 释放全局锁
  root_latch_.unlock();

  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t next_page_id;

    if (left_most) {
      next_page_id = internal->ValueAt(0);
    } else {
      next_page_id = internal->Lookup(key, comparator_);
    }

    auto *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (op == Operation::READ) {
      // 读操作：Read Latch-Coupling，总是安全
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      // 插入/删除操作：Write Latch-Coupling
      child_page->WLatch();

      bool is_safe = false;
      if (op == Operation::INSERT) {
        // 插入安全条件：子节点非满 (Max - 1)
        is_safe = child_node->GetSize() < child_node->GetMaxSize() - 1;
      } else { // op == Operation::DELETE
        // 删除安全条件：子节点未达最小大小 (MinSize + 1)
        is_safe = child_node->GetSize() > child_node->GetMinSize();
      }

      if (is_safe) {
        // 安全：释放所有祖先锁和当前页锁
        ReleaseLatchFromQueue(buffer_pool_manager_, transaction);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      } else {
        // 不安全：保留当前页的写锁，以便分裂/合并时使用
        if (transaction != nullptr) {
          transaction->AddIntoPageSet(page);
        }
      }
    }

    page = child_page;
    node = child_node;
  }

  return page;
}

/**
 * 初始化新树：创建第一个叶节点并设置为根。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "NewPage failed");
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);

  root_page_id_ = new_page_id;
  UpdateRootPageId(1); // 插入新记录

  leaf->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/**
 * 分裂节点（叶节点或内部节点）。
 * @param node 待分裂的节点（已满）
 * @return 新分配的节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception("Split failed: cannot allocate new page");
  }

  auto *new_node = reinterpret_cast<N *>(page->GetData());

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);
    new_leaf->Init(new_page_id, leaf->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(new_leaf);
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);
    new_internal->Init(new_page_id, internal->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }

  return new_node;
}

/**
 * 将分裂键插入到父节点中。
 * 如果父节点也满，则递归地向上分裂。
 * @param old_node 分裂前的节点
 * @param key 上推到父节点的键
 * @param new_node 分裂出的新节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::InsertIntoParent(N *old_node, const KeyType &key, N *new_node, Transaction *transaction) {
  page_id_t parent_id = old_node->GetParentPageId();

  // 1. 处理根节点分裂 (old_node 是旧根)
  if (parent_id == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    auto *page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (page == nullptr) {
      throw Exception("InsertIntoParent failed: cannot allocate new root page");
    }

    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetSize(2);
    new_root->SetValueAt(0, old_node->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, new_node->GetPageId());

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);

    // 修复：此时我们已经持有 old_node 的 WLatch，不应该再请求 root_latch_。
    // 根页 ID 的更新应该在锁的保护下，但在单线程下可以简化。
    // 在并发实现中，FindLeafPage 已经释放了全局锁，更新 root_page_id_ 是在 old_node WLatch 保护下的。
    // 在 B+ 树并发实现中，只有在 Insert/Remove 开始时才尝试锁 root_latch_。
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0); // 更新现有记录

    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }

  // 2. 插入到现有父节点
  // 父节点在 FindLeafPage 中已被加 WLatch 并保留在 transaction->PageSet 中
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 此时 parent page 应该仍然保持 pinned 状态（因为它在 transaction->PageSet 中）

  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parent_id);

  // 3. 检查父节点是否需要分裂 (递归)
  if (parent->GetSize() >= parent->GetMaxSize()) {
    auto *new_internal = Split<InternalPage>(parent);
    KeyType up_key = new_internal->KeyAt(1); // 内部节点分裂时，中间键上推
    InsertIntoParent(parent, up_key, new_internal, transaction);
    buffer_pool_manager_->UnpinPage(new_internal->GetPageId(), true);
  }
  // 注意：parent_page 的锁和 Unpin 留给 UnlockUnpinPages 处理
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/**
 * 删除后的合并或重新分配逻辑。
 * @param node 当前节点
 * @return 是否导致父节点大小变化 (true)
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // 1. 根节点特殊处理
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // 2. 已满足最小大小，无需操作
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }

  // 3. 查找父节点和邻居节点
  page_id_t parent_id = node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // 父节点已被 FindLeafPage 加 WLatch 并保留在 transaction->PageSet 中

  int index = parent->ValueIndex(node->GetPageId());
  assert(index >= 0);

  int neighbor_index = (index == 0 ? 1 : index - 1); // 优先选左邻居 (index - 1)
  page_id_t neighbor_page_id = parent->ValueAt(neighbor_index);
  auto *neighbor_page = buffer_pool_manager_->FetchPage(neighbor_page_id);
  auto *neighbor = reinterpret_cast<N *>(neighbor_page->GetData());
  // 邻居节点也需要在 FindLeafPage 中被加 WLatch

  // 4. 检查是否可以重新分配
  if (neighbor->GetSize() > neighbor->GetMinSize()) {
    Redistribute(neighbor, node, index);
    buffer_pool_manager_->UnpinPage(neighbor_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return false; // 大小未变，无需向上层传递
  }

  // 5. 执行合并
  bool result = Coalesce(neighbor, node, parent, index, transaction);

  buffer_pool_manager_->UnpinPage(neighbor_page_id, true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
  return result; // 返回是否继续向上合并
}

/**
 * 将节点与邻居合并。
 * @param neighbor_node 邻居节点 (目标合并节点)
 * @param node 待删除的节点
 * @param parent 共同的父节点
 * @param index 当前节点在父节点中的索引
 * @return 是否需要向上递归合并
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index, Transaction *transaction)
    -> bool {
  int node_index = parent->ValueIndex(node->GetPageId());
  int neighbor_index = parent->ValueIndex(neighbor_node->GetPageId());
  assert(node_index >= 0 && neighbor_index >= 0);

  bool neighbor_is_left = neighbor_index < node_index;

  // 确保 node 总是合并到 neighbor_node 中（即 neighbor_node 是左侧节点）
  if (!neighbor_is_left) {
    std::swap(node, neighbor_node);
    std::swap(node_index, neighbor_index);
  }

  // 叶节点合并
  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);

    // 将 leaf 的所有内容移动到 neighbor_leaf
    leaf->MoveAllTo(neighbor_leaf);
    neighbor_leaf->SetNextPageId(leaf->GetNextPageId());

    // 从父节点中移除 leaf 的引用
    parent->Remove(node_index);
    buffer_pool_manager_->DeletePage(leaf->GetPageId());
  }
  // 内部节点合并
  else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);

    // 将 internal 的内容及父节点中的分隔键移动到 neighbor_internal
    internal->MoveAllTo(neighbor_internal, node_index, buffer_pool_manager_);

    // 从父节点中移除 internal 的引用
    parent->Remove(node_index);
    buffer_pool_manager_->DeletePage(internal->GetPageId());
  }

  // 检查父节点是否需要调整 (递归)
  if (parent->IsRootPage()) {
    return AdjustRoot(parent);
  }

  // 检查父节点是否达到最小大小
  if (parent->GetSize() < parent->GetMinSize()) {
    // 递归向上执行合并或重新分配
    return CoalesceOrRedistribute(parent, transaction);
  }

  return true;
}

/**
 * 从邻居节点重新分配一个键值对给当前节点。
 * @param neighbor_node 邻居节点
 * @param node 当前节点 (已达最小大小)
 * @param index 当前节点在父节点中的索引
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  page_id_t parent_id = node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int node_index = parent->ValueIndex(node->GetPageId());
  int neighbor_index = parent->ValueIndex(neighbor_node->GetPageId());
  assert(node_index >= 0 && neighbor_index >= 0);

  bool neighbor_is_left = neighbor_index < node_index;

  // 叶节点重新分配
  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);

    if (neighbor_is_left) {
      // 从左邻居的末尾移动到当前节点的开头
      neighbor_leaf->MoveLastToFrontOf(leaf);
      // 更新父节点中指向当前节点的新分隔键
      parent->SetKeyAt(node_index, leaf->KeyAt(0));
    } else {
      // 从右邻居的开头移动到当前节点的末尾
      neighbor_leaf->MoveFirstToEndOf(leaf);
      // 更新父节点中指向邻居节点的新分隔键
      parent->SetKeyAt(neighbor_index, neighbor_leaf->KeyAt(0));
    }
  }
  // 内部节点重新分配
  else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);

    if (neighbor_is_left) {
      // 从左邻居的末尾移动到当前节点的开头
      KeyType middle_key = parent->KeyAt(node_index);
      neighbor_internal->MoveLastToFrontOf(internal, middle_key, buffer_pool_manager_);
      // 更新父节点中的分隔键
      parent->SetKeyAt(node_index, neighbor_internal->KeyAt(neighbor_internal->GetSize() - 1));
    } else {
      // 从右邻居的开头移动到当前节点的末尾
      KeyType middle_key = parent->KeyAt(neighbor_index);
      neighbor_internal->MoveFirstToEndOf(internal, middle_key, buffer_pool_manager_);
      // 更新父节点中的分隔键
      parent->SetKeyAt(neighbor_index, internal->KeyAt(internal->GetSize() - 1));
    }
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/**
 * 删除或合并后的根节点调整。
 * @param old_root_node 旧根节点
 * @return 是否进行了根节点调整 (true)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  // 1. 叶根节点
  if (old_root_node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(old_root_node);
    if (leaf->GetSize() == 0) {
      // 叶根为空，删除根节点，树变为空
      buffer_pool_manager_->DeletePage(leaf->GetPageId());
      std::lock_guard<std::mutex> lock(root_latch_);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      return true;
    }
    return true; // 叶根非空，无需调整
  }

  // 2. 内部根节点
  auto *root_internal = reinterpret_cast<InternalPage *>(old_root_node);
  if (root_internal->GetSize() > 1) {
    return false; // 内部根节点包含多于一个指针，无需调整
  }

  // 内部根节点只剩一个子节点，子节点成为新根
  page_id_t child_page_id = root_internal->RemoveAndReturnOnlyChild();
  auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

  child_node->SetParentPageId(INVALID_PAGE_ID);

  // 更新全局根页 ID
  std::lock_guard<std::mutex> lock(root_latch_);
  root_page_id_ = child_page_id;
  UpdateRootPageId(0);

  buffer_pool_manager_->UnpinPage(child_page_id, true);
  buffer_pool_manager_->DeletePage(root_internal->GetPageId());

  return true;
}

/* 从文件插入（仅用于测试）*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/* 从文件移除（仅用于测试）*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

// Draw tree (debug only)
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  auto *root_page = bpm->FetchPage(root_page_id_);
  if (root_page == nullptr) {
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(root_page->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();

  bpm->UnpinPage(root_page_id_, false);
}

// Print tree (debug only)
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  auto *root_page = bpm->FetchPage(root_page_id_);
  if (root_page == nullptr) {
    return;
  }

  ToString(reinterpret_cast<BPlusTreePage *>(root_page->GetData()), bpm);
  bpm->UnpinPage(root_page_id_, false);
}

// ToGraph (debug only)
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    out << leaf_prefix << leaf->GetPageId();
    out << "[shape=plain color=green ";
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    out << "</TABLE>>];\n";

    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    out << internal_prefix << inner->GetPageId();
    out << "[shape=plain color=pink ";
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    out << "</TABLE>>];\n";

    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }

    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

// ToString (debug only)
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

// 模板实例化
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub