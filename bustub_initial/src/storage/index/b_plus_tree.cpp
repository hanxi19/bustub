#include <string>

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
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
// 补充：通过判断 root_page_id_ 是否为 INVALID_PAGE_ID 来判断整棵树是否为空
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 补充：若树为空，直接返回 false
  if (IsEmpty()) {
    return false;
  }

  // 补充：通过 FindLeafPage 在叶子页中查找 key
  auto *leaf = FindLeafPage(key, false, transaction);
  if (leaf == nullptr) {
    return false;
  }

  ValueType value;
  bool found = leaf->Lookup(key, value, comparator_);
  if (found) {
    result->clear();
    result->push_back(value);
  }

  // 查找结束后取消固定叶子页
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 补充：若树为空，则创建新树并插入第一条记录
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  // 补充：非空时执行正常插入流程
  return InsertIntoLeaf(key, value, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：从叶子结点删除给定 key，并在必要时向上执行合并/重分配以维持 B+Tree 性质
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 空树，直接返回
  if (IsEmpty()) {
    return;
  }

  // 在对应叶子页中执行删除
  auto *leaf = FindLeafPage(key, false, transaction);
  if (leaf == nullptr) {
    return;
  }

  int old_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, comparator_);

  // key 不存在，什么也不做
  if (new_size == old_size) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }

  // 如果删除后叶子仍满足最小容量，结束
  if (leaf->IsRootPage() || new_size >= leaf->GetMinSize()) {
    // 可能是根页（单页树），此时无需合并
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }

  // 该叶子页小于最小容量，尝试与兄弟结点合并或重分配
  CoalesceOrRedistribute(leaf, transaction);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：返回指向整棵 B+Tree 最左叶子第一个键值对的迭代器；若树为空，则返回 end 迭代器
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  // 查找最左叶子页
  KeyType dummy_key{};
  auto *leaf = FindLeafPage(dummy_key, true, nullptr);
  if (leaf == nullptr || leaf->GetSize() == 0) {
    if (leaf != nullptr) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    return End();
  }

  page_id_t leaf_page_id = leaf->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：返回指向第一个 key >= 给定 key 的迭代器；若不存在这样的 key，则返回 end
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  auto *leaf = FindLeafPage(key, false, nullptr);
  if (leaf == nullptr) {
    return End();
  }

  int index = leaf->KeyIndex(key, comparator_);
  page_id_t leaf_page_id = leaf->GetPageId();

  // 如果在该叶子中 index 已越界，则跳到下一叶子
  if (index >= leaf->GetSize()) {
    page_id_t next_page_id = leaf->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    if (next_page_id == INVALID_PAGE_ID) {
      return End();
    }
    return INDEXITERATOR_TYPE(buffer_pool_manager_, next_page_id, 0);
  }

  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：返回一个 end 迭代器，表示遍历结束
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
// 补全：直接返回当前记录的根节点 page_id_
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*****************************************************************************
 * HELPER FUNCTIONS (FIND / INSERT / SPLIT / DELETE)
 *****************************************************************************/

/**
 * 补充：从根节点开始查找叶子页
 * left_most=true 时一路走到最左叶子（用于 Begin），否则按照给定 key 做查找
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool left_most, Transaction *transaction) -> LeafPage * {
  if (IsEmpty()) {
    return nullptr;
  }

  page_id_t page_id = root_page_id_;
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // 自顶向下，直到遇到叶子页
  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t next_page_id;
    if (left_most) {
      // 最左叶子：每次都走最左 child
      next_page_id = internal->ValueAt(0);
    } else {
      // 正常查找：根据 key 选择 child
      next_page_id = internal->Lookup(key, comparator_);
    }

    page_id_t cur_page_id = node->GetPageId();
    // 下探之前，取消固定当前内部节点
    buffer_pool_manager_->UnpinPage(cur_page_id, false);

    page = buffer_pool_manager_->FetchPage(next_page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  // 最后一层为叶子页，由调用者负责 Unpin
  return reinterpret_cast<LeafPage *>(node);
}

/**
 * 补充：当前树为空时，创建一棵只包含一个叶子页的新树，并插入第一条 (key, value)
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception("StartNewTree failed: cannot allocate new page");
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf->Insert(key, value, comparator_);

  root_page_id_ = page_id;
  // 第一次创建根节点，向 HeaderPage 中插入记录
  UpdateRootPageId(1);

  buffer_pool_manager_->UnpinPage(page_id, true);
}

/**
 * 补充：在非空 B+Tree 中，将 (key, value) 插入合适的叶子页；若 key 已存在返回 false
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto *leaf = FindLeafPage(key, false, transaction);
  if (leaf == nullptr) {
    return false;
  }

  ValueType existing;
  if (leaf->Lookup(key, existing, comparator_)) {
    // 唯一键约束：不允许插入重复 key
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }

  leaf->Insert(key, value, comparator_);

  // 若插入后仍未达到最大容量（严格小于 max_size），直接结束
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return true;
  }

  // 发生溢出，需要分裂叶子页
  auto *new_leaf = Split<LeafPage>(leaf);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());

  // 将新叶子第一个 key 作为分裂键插入父节点
  KeyType split_key = new_leaf->KeyAt(0);
  InsertIntoParent(leaf, split_key, new_leaf, transaction);

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  return true;
}

/**
 * 补充：当节点已满时，将其分裂为两个节点，返回新分裂出来的节点指针
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
    // 叶子页分裂
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);
    new_leaf->Init(new_page_id, leaf->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(new_leaf);
  } else {
    // 内部页分裂
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);
    new_internal->Init(new_page_id, internal->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }

  return new_node;
}

/**
 * 补充：在 old_node 分裂出 new_node 后，把分裂键插入父节点；必要时父节点也可能继续分裂
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::InsertIntoParent(N *old_node, const KeyType &key, N *new_node, Transaction *transaction) {
  page_id_t parent_id = old_node->GetParentPageId();

  // 如果原先是根节点，则创建一个新的根内部结点
  if (parent_id == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    auto *page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (page == nullptr) {
      throw Exception("InsertIntoParent failed: cannot allocate new root page");
    }

    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);

    // 新根的 size 应为 2：
    // slot 0: 无效 key，占位，value 为 old_node
    // slot 1: key, value 为 new_node
    new_root->SetSize(2);
    new_root->SetValueAt(0, old_node->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, new_node->GetPageId());

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);

    root_page_id_ = new_root_page_id;
    // 根已存在记录，仅需更新 root_page_id
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }

  // 一般情况：向现有父节点插入分裂键
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parent_id);

  // 若父节点达到最大容量（size >= max_size），则继续向上分裂
  if (parent->GetSize() >= parent->GetMaxSize()) {
    auto *new_internal = Split<InternalPage>(parent);
    // 分裂后，新 internal 的第一个有效 key 作为分裂键插入其父
    KeyType up_key = new_internal->KeyAt(1);
    InsertIntoParent(parent, up_key, new_internal, transaction);
    buffer_pool_manager_->UnpinPage(new_internal->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/**
 * 补充：删除后若节点小于最小容量，尝试与兄弟结点合并或重分配，返回是否树结构发生了变化
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // 根结点单独处理
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // 若当前结点仍满足最小容量，则不需要进一步处理
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }

  page_id_t parent_id = node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int index = parent->ValueIndex(node->GetPageId());
  assert(index >= 0);

  // 选择一个兄弟结点：优先选择左兄弟，否则选择右兄弟
  int neighbor_index = (index == 0 ? 1 : index - 1);
  page_id_t neighbor_page_id = parent->ValueAt(neighbor_index);
  auto *neighbor_page = buffer_pool_manager_->FetchPage(neighbor_page_id);
  auto *neighbor = reinterpret_cast<N *>(neighbor_page->GetData());

  // 如果兄弟结点有富余元素，尝试重分配
  if (neighbor->GetSize() > neighbor->GetMinSize()) {
    Redistribute(neighbor, node, index);
    buffer_pool_manager_->UnpinPage(neighbor_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return false;
  }

  // 否则只能与兄弟进行合并
  bool result = Coalesce(neighbor, node, parent, index, transaction);

  buffer_pool_manager_->UnpinPage(neighbor_page_id, true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
  return result;
}

/**
 * 补充：将 node 与 neighbor_node 进行真正的合并（由 parent 和 index 决定左右关系）
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index, Transaction *transaction)
    -> bool {
  // 通过 parent 确定两者在父结点中的位置关系
  int node_index = parent->ValueIndex(node->GetPageId());
  int neighbor_index = parent->ValueIndex(neighbor_node->GetPageId());
  assert(node_index >= 0 && neighbor_index >= 0);

  bool neighbor_is_left = neighbor_index < node_index;

  if (node->IsLeafPage()) {
    // 叶子结点合并
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);

    if (neighbor_is_left) {
      // 左兄弟 + 当前叶子：将当前叶子全部移动到左兄弟后面
      leaf->MoveAllTo(neighbor_leaf);
      neighbor_leaf->SetNextPageId(leaf->GetNextPageId());
      parent->Remove(node_index);
      buffer_pool_manager_->DeletePage(leaf->GetPageId());
    } else {
      // 当前叶子 + 右兄弟：将右兄弟全部移动到当前叶子后面
      neighbor_leaf->MoveAllTo(leaf);
      leaf->SetNextPageId(neighbor_leaf->GetNextPageId());
      parent->Remove(neighbor_index);
      buffer_pool_manager_->DeletePage(neighbor_leaf->GetPageId());
    }
  } else {
    // 内部结点合并
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);

    if (neighbor_is_left) {
      // 左兄弟 + 当前内部结点：把当前内部结点所有条目移动到左兄弟末尾
      internal->MoveAllTo(neighbor_internal, node_index, buffer_pool_manager_);
      parent->Remove(node_index);
      buffer_pool_manager_->DeletePage(internal->GetPageId());
    } else {
      // 当前内部结点 + 右兄弟：把右兄弟所有条目移动到当前结点末尾
      neighbor_internal->MoveAllTo(internal, neighbor_index, buffer_pool_manager_);
      parent->Remove(neighbor_index);
      buffer_pool_manager_->DeletePage(neighbor_internal->GetPageId());
    }
  }

  // 合并完成后，若父结点是根或变得过小，需要进一步向上调整
  if (parent->IsRootPage()) {
    return AdjustRoot(parent);
  }

  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }

  return true;
}

/**
 * 补充：在不合并的情况下，从 neighbor_node 借一个键值对重分配给 node
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

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);

    if (neighbor_is_left) {
      // 左兄弟 -> 当前结点：从左兄弟借最后一个元素，放到当前结点开头
      neighbor_leaf->MoveLastToFrontOf(leaf);
      // 更新父结点中分隔 key，使其等于当前结点的第一个 key
      parent->SetKeyAt(node_index, leaf->KeyAt(0));
    } else {
      // 右兄弟 -> 当前结点：从右兄弟借第一个元素，放到当前结点末尾
      neighbor_leaf->MoveFirstToEndOf(leaf);
      // 更新父结点中分隔 key，使其等于右兄弟新的第一个 key
      parent->SetKeyAt(neighbor_index, neighbor_leaf->KeyAt(0));
    }
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);

    if (neighbor_is_left) {
      // 左兄弟 -> 当前内部结点：使用父结点的分隔 key，将左兄弟最后一个 child 旋转到当前结点前面
      KeyType middle_key = parent->KeyAt(node_index);
      neighbor_internal->MoveLastToFrontOf(internal, middle_key, buffer_pool_manager_);
      // 父结点新的分隔 key 取自左兄弟的最后一个 key
      parent->SetKeyAt(node_index, neighbor_internal->KeyAt(neighbor_internal->GetSize() - 1));
    } else {
      // 右兄弟 -> 当前内部结点：使用父结点的分隔 key，将右兄弟第一个 child 旋转到当前结点末尾
      KeyType middle_key = parent->KeyAt(neighbor_index);
      neighbor_internal->MoveFirstToEndOf(internal, middle_key, buffer_pool_manager_);
      // 父结点新的分隔 key 可以取当前结点最后一个 key 作为新的边界键
      parent->SetKeyAt(neighbor_index, internal->KeyAt(internal->GetSize() - 1));
    }
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/**
 * 补充：在删除导致根节点过小或为空时，调整 root_page_id_（可能降低树高）
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  // 根是叶子结点的情况
  if (old_root_node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(old_root_node);
    if (leaf->GetSize() == 0) {
      // 整棵树被清空
      buffer_pool_manager_->DeletePage(leaf->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    }
    return true;
  }

  // 根是内部结点
  auto *root_internal = reinterpret_cast<InternalPage *>(old_root_node);

  if (root_internal->GetSize() > 1) {
    // 仍然有两个及以上孩子，无需改变高度
    return false;
  }

  // 只有一个孩子，将该孩子提升为新的根
  page_id_t child_page_id = root_internal->RemoveAndReturnOnlyChild();
  auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

  child_node->SetParentPageId(INVALID_PAGE_ID);
  root_page_id_ = child_page_id;
  UpdateRootPageId(0);

  buffer_pool_manager_->UnpinPage(child_page_id, true);
  buffer_pool_manager_->DeletePage(root_internal->GetPageId());

  return true;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
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
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
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

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
