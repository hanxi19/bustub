//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
// 补全：根据 page_type_ 判断当前页是否为叶子页
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }

// 补全：以 parent_page_id_ 是否为 INVALID_PAGE_ID 来判断是否为根页
auto BPlusTreePage::IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }

// 补全：设置页类型到 page_type_ 成员
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
// 补全：直接返回当前 size_（KV 对数量）
auto BPlusTreePage::GetSize() const -> int { return size_; }

// 补全：将 size_ 设置为给定大小
void BPlusTreePage::SetSize(int size) { size_ = size; }

// 补全：在当前 size_ 基础上累加 amount
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
// 补全：返回页的最大容量 max_size_
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }

// 补全：设置页的最大容量 max_size_
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
// 补全：最小页大小定义为 max_size_ / 2
auto BPlusTreePage::GetMinSize() const -> int { return max_size_ / 2; }

/*
 * Helper methods to get/set parent page id
 */
// 补全：返回父页的 page_id（parent_page_id_）
auto BPlusTreePage::GetParentPageId() const -> page_id_t { return parent_page_id_; }

// 补全：更新父页的 page_id（parent_page_id_）
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
// 补全：返回当前页自身的 page_id_
auto BPlusTreePage::GetPageId() const -> page_id_t { return page_id_; }

// 补全：设置当前页自身的 page_id_
void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}  // namespace bustub
