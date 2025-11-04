#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
using frame_id_t = int;
const frame_id_t INVALID_FRAME_ID = -1;
namespace bustub {

// 构造函数：初始化替换器大小和 K 值
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  BUSTUB_ASSERT(k_ >= 1, "LRU-K requires k >= 1");  // 确保 K 为正整数
}

/**
 * @brief 淘汰后退 k-距离最大的可淘汰帧
 * @return 成功淘汰返回 true，无可用帧返回 false
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // 无可用可淘汰帧，直接返回
  if (curr_size_ == 0) {
    return false;
  }

  frame_id_t victim_fid = INVALID_FRAME_ID;
  size_t max_backward_dist = 0;                // 最大后退 k-距离
  size_t earliest_first_ts = std::numeric_limits<size_t>::max();  // 访问次数<k时的最早访问时间

  // 遍历所有帧，筛选可淘汰帧并寻找受害者
  for (const auto &[fid, finfo] : frame_map_) {
    if (!finfo.evictable) {
      continue;  // 跳过不可淘汰帧
    }

    const auto &ts_list = finfo.access_timestamps;
    size_t backward_dist;

    if (ts_list.size() < k_) {
      // 情况1：访问次数 < k，后退距离为 +∞
      backward_dist = std::numeric_limits<size_t>::max();
      size_t first_ts = ts_list.front();  // 最早访问时间

      // 优先淘汰 +∞ 距离的帧，若多个则选最早访问的
      if (backward_dist > max_backward_dist) {
        max_backward_dist = backward_dist;
        earliest_first_ts = first_ts;
        victim_fid = fid;
      } else if (backward_dist == max_backward_dist && first_ts < earliest_first_ts) {
        earliest_first_ts = first_ts;
        victim_fid = fid;
      }
    } else {
      // 情况2：访问次数 >= k，后退距离 = 当前时间戳 - 第 k 次访问时间戳
      size_t kth_ts = ts_list.front();  // 第 k 次访问时间戳（列表最早元素）
      backward_dist = current_timestamp_ - kth_ts;

      // 选择后退距离最大的帧
      if (backward_dist > max_backward_dist) {
        max_backward_dist = backward_dist;
        victim_fid = fid;
      }
    }
  }

  // 断言：curr_size_>0 时必存在受害者（避免逻辑错误）
  BUSTUB_ASSERT(victim_fid != INVALID_FRAME_ID, "Evict failed: no victim found but curr_size>0");

  // 输出受害者帧ID，删除其元数据，更新可淘汰数量
  *frame_id = victim_fid;
  frame_map_.erase(victim_fid);
  curr_size_--;

  return true;
}

/**
 * @brief 记录帧的访问（更新时间戳）
 * @param frame_id 被访问的帧ID（需在 [0, replacer_size_) 范围内）
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 检查帧ID有效性（超出替换器大小为无效）
  BUSTUB_ASSERT(frame_id < replacer_size_, "RecordAccess failed: invalid frame_id (out of range)");

  // 获取或创建帧元数据
  auto &frame_info = frame_map_[frame_id];
  // 记录当前访问时间戳
  frame_info.access_timestamps.push_back(current_timestamp_);
  // 仅保留最近的 k 个时间戳（超过则删除最早的）
  if (frame_info.access_timestamps.size() > k_) {
    frame_info.access_timestamps.pop_front();
  }

  // 时间戳递增（确保每次访问的时间唯一性）
  current_timestamp_++;
}

/**
 * @brief 切换帧的可淘汰状态（更新可淘汰数量）
 * @param frame_id 目标帧ID
 * @param set_evictable 目标状态（true=可淘汰，false=不可淘汰）
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  // 检查帧ID有效性
  BUSTUB_ASSERT(frame_id < replacer_size_, "SetEvictable failed: invalid frame_id (out of range)");

  // 帧不存在则直接返回（无需修改）
  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    return;
  }

  auto &frame_info = it->second;
  // 状态无变化则返回
  if (frame_info.evictable == set_evictable) {
    return;
  }

  // 更新可淘汰数量
  if (set_evictable) {
    curr_size_++;  // 不可淘汰 → 可淘汰：数量+1
  } else {
    curr_size_--;  // 可淘汰 → 不可淘汰：数量-1
  }

  // 更新帧的可淘汰状态
  frame_info.evictable = set_evictable;
}

/**
 * @brief 删除可淘汰帧（移除其访问历史）
 * @param frame_id 目标帧ID（必须是可淘汰的）
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 检查帧ID有效性
  BUSTUB_ASSERT(frame_id < replacer_size_, "Remove failed: invalid frame_id (out of range)");

  // 帧不存在则直接返回
  auto it = frame_map_.find(frame_id);
  if (it == frame_map_.end()) {
    return;
  }

  auto &frame_info = it->second;
  // 断言：仅可淘汰帧可被删除
  BUSTUB_ASSERT(frame_info.evictable, "Remove failed: cannot remove non-evictable frame");

  // 减少可淘汰数量，删除帧元数据
  curr_size_--;
  frame_map_.erase(it);
}

/**
 * @brief 返回当前可淘汰帧的数量
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub