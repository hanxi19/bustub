#pragma once
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);
  DISALLOW_COPY_AND_MOVE(LRUKReplacer);
  ~LRUKReplacer() = default;

  auto Evict(frame_id_t *frame_id) -> bool;
  void RecordAccess(frame_id_t frame_id);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;

 private:
  // 帧元数据结构体：存储访问历史和可淘汰状态
  struct FrameInfo {
    std::list<size_t> access_timestamps;  // 访问时间戳（最早在前，最近在后）
    bool evictable;                       // 是否可淘汰（默认不可淘汰）
    FrameInfo() : evictable(false) {}
  };

  std::unordered_map<frame_id_t, FrameInfo> frame_map_;  // frame_id -> 帧元数据
  size_t current_timestamp_{0};                          // 全局时间戳（每次访问递增）
  size_t curr_size_{0};                                  // 可淘汰帧的数量
  size_t replacer_size_;                                 // 替换器最大管理帧数
  size_t k_;                                             // LRU-K 的 K 值
  std::mutex latch_;                                     // 线程安全锁
};
}  // namespace bustub