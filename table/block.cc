// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include "table/block.h"

#include <vector>
#include <algorithm>
#include "leveldb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// 读取Restart的个数：最后4个字节
inline uint32_t Block::NumRestarts() const {
  assert(size_ >= sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

/**
 * 从BlockContents中解析出对应的kv数据和restart数据，
 * 并提供迭代去进行kv数据的遍历操作
 */
Block::Block(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated) {
  if (size_ < sizeof(uint32_t)) {
    // 至少需要4个字节,用于记录restart的个数,否则意味数据错误
    size_ = 0;  // Error marker
  } else {
    // 如果所有的空间都用于记录restart个数,计算出最多可以有多少个restart
    size_t max_restarts_allowed = (size_-sizeof(uint32_t)) / sizeof(uint32_t);
    if (NumRestarts() > max_restarts_allowed) {
      // The size is too small for NumRestarts()
      // 从restart个数字段读取的个数大于所有空间都用于存储restar的(最大)个数，意味着数据错误
      size_ = 0;
    } else {
      // restart的偏移量 = size_ - [最后4个字节(代表restart个数) - restart个数]*4(每个restart是一个指向具体key的4字节偏移量)
      restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
    }
  }
}

Block::~Block() {
  if (owned_) {
    delete[] data_;
  }
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not derefence past "limit".
//
// If any errors are detected, returns NULL.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
// 返回指向key差异的部分地址，每个kv的存储格式可以看block_builder.cc的add函数
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared,
                                      uint32_t* non_shared,
                                      uint32_t* value_length) {
  if (limit - p < 3) return NULL;
  *shared = reinterpret_cast<const unsigned char*>(p)[0];
  *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
  *value_length = reinterpret_cast<const unsigned char*>(p)[2];
  if ((*shared | *non_shared | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 3;
  } else {
    if ((p = GetVarint32Ptr(p, limit, shared)) == NULL) return NULL;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == NULL) return NULL;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == NULL) return NULL;
  }

  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
    return NULL;
  }
  return p;
}

class Block::Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  const char* const data_;      // underlying block contents
  uint32_t const restarts_;     // Offset of restart array (list of fixed32)
  uint32_t const num_restarts_; // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  std::string key_;
  Slice value_;
  Status status_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  // 返回当前整个条目entry的大小，也就相当于下一个条目的offset
  inline uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

  // 获取指定index的restart的值，其指向一个拥有16(配置的间隔多少数据一个重启点)个kv的数据(组)
  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = Slice(data_ + offset, 0);
  }

 public:
  Iter(const Comparator* comparator,
       const char* data,
       uint32_t restarts,
       uint32_t num_restarts)
      : comparator_(comparator),
        data_(data),
        restarts_(restarts),
        num_restarts_(num_restarts),
        current_(restarts_),
        restart_index_(num_restarts_) {
    assert(num_restarts_ > 0);
  }

  virtual bool Valid() const { return current_ < restarts_; }
  virtual Status status() const { return status_; }
  virtual Slice key() const {
    assert(Valid());
    return key_;
  }
  virtual Slice value() const {
    assert(Valid());
    return value_;
  }

  virtual void Next() {
    assert(Valid());
    ParseNextKey();
  }

  virtual void Prev() {
    assert(Valid());

    // Scan backwards to a restart point before current_
    const uint32_t original = current_;
    while (GetRestartPoint(restart_index_) >= original) {
      if (restart_index_ == 0) {
        // No more entries
        current_ = restarts_;
        restart_index_ = num_restarts_;
        return;
      }
      restart_index_--;
    }

    SeekToRestartPoint(restart_index_);
    do {
      // Loop until end of current entry hits the start of original entry
    } while (ParseNextKey() && NextEntryOffset() < original);
  }

  // 先通过restarts二分查找到对应的组(region),然后在region中线性查找(注意：本接口是查找到结束，不直接返回结果，而是要通过判断Iter的状态来看是否有找到对应的key)
  virtual void Seek(const Slice& target) {
    // Binary search in restart array to find the last restart point
    // with a key < target
    uint32_t left = 0;
    uint32_t right = num_restarts_ - 1;
    while (left < right) {
      uint32_t mid = (left + right + 1) / 2;
      uint32_t region_offset = GetRestartPoint(mid);
      uint32_t shared, non_shared, value_length;
      // 获取key中non_shared部分的地址
      const char* key_ptr = DecodeEntry(data_ + region_offset,  // mid 对应的数据组(region)的开始位置
                                        data_ + restarts_,      // restart数组的开始位置 
                                        &shared, &non_shared, &value_length);
      if (key_ptr == NULL || (shared != 0)) {
        CorruptionError();
        return;
      }
      // 读取key中non_shared部分的字符串(每個region第一key的non_shared就是完整的key)
      Slice mid_key(key_ptr, non_shared);
      if (Compare(mid_key, target) < 0) {
        // Key at "mid" is smaller than "target".  Therefore all
        // blocks before "mid" are uninteresting.
        left = mid;
      } else {
        // Key at "mid" is >= "target".  Therefore all blocks at or
        // after "mid" are uninteresting.
        right = mid - 1;
      }
    }

    // Linear search (within restart block) for first key >= target
    // 找到了对应的组(region)，在分组内线性查找
    SeekToRestartPoint(left);
    while (true) {
      if (!ParseNextKey()) {
        return;
      }
      if (Compare(key_, target) >= 0) {
        return;
      }
    }
  }

  virtual void SeekToFirst() {
    SeekToRestartPoint(0);
    ParseNextKey();
  }

  virtual void SeekToLast() {
    SeekToRestartPoint(num_restarts_ - 1);
    while (ParseNextKey() && NextEntryOffset() < restarts_) {
      // Keep skipping
    }
  }

 private:
  void CorruptionError() {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    key_.clear();
    value_.clear();
  }

  bool ParseNextKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restarts_;  // Restarts come right after data
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    // Decode next entry
    uint32_t shared, non_shared, value_length;
    // 获取key中non_shared部分的地址
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
    if (p == NULL || key_.size() < shared) {
      CorruptionError();
      return false;
    } else {
      key_.resize(shared);
      // 读取key的非共享部分
      key_.append(p, non_shared);
      // 读取value
      value_ = Slice(p + non_shared, value_length);
      while (restart_index_ + 1 < num_restarts_ &&
             GetRestartPoint(restart_index_ + 1) < current_) {
        ++restart_index_;
      }
      return true;
    }
  }
};

Iterator* Block::NewIterator(const Comparator* cmp) {
  if (size_ < sizeof(uint32_t)) {
    return NewErrorIterator(Status::Corruption("bad block contents"));
  }
  const uint32_t num_restarts = NumRestarts();
  if (num_restarts == 0) {
    return NewEmptyIterator();
  } else {
    return new Iter(cmp, data_, restart_offset_, num_restarts);
  }
}

}  // namespace leveldb
