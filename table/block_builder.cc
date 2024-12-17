// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
// 
// 存储方案：
//  1.key存储不同部分进行空间节省；而value部分不进行压缩
//  2.block按group存储（也就是重启点restart），同一个group内的key跟本group第一个key对比，只存储差异的部分
// An entry for a particular key-value pair has the form: （布局如下）
//     shared_bytes: varint32    (跟本group第一个key相同的部分的长度)
//     unshared_bytes: varint32  (跟本group第一个key不同的部分的长度)
//     value_length: varint32    (value值的长度，value不压缩)
//     key_delta: char[unshared_bytes] （value不同部分的数据内容）
//     value: char[value_length] （value值具体的数据内容）
// shared_bytes == 0 for restart points.  （对于restart point， 不压缩，所以shared_bytes == 0）
//
// The trailer of the block has the form: (block的尾部布局如下）
//     restarts: uint32[num_restarts]  （restart数组，是一个offset，指向group的开始地址）
//     num_restarts: uint32 （restart的数量）
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

// 增加restart到buffer_
Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    // 计算与上一个key相同的字节数
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    // 增加新的重启点
    restarts_.push_back(buffer_.size());
    // 重置当前存储点的kv数量
    counter_ = 0;
  }
  // 计算非共享的字节数
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  // 写非共享的部分
  buffer_.append(key.data() + shared, non_shared);
  // 写整个value
  buffer_.append(value.data(), value.size());

  // Update state
  // 将last_key_更新为当前key，这里做了小优化，先保留相同的部分，其他的丢掉，再增加差异的部分
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
