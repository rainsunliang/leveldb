// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

struct Options;


// Block的布局
// |-----------------------------------------------|    <- buffer_            
// |                 k-v-1                         |                          
// |-----------------------------------------------|                          
// |                 k-v-2                         |                          
// |-----------------------------------------------|                            
// |                 k-v-n                         |                            
// |-----------------------------------------------|      }
// |                 restart-0                     |      |
// |-----------------------------------------------|      |
// |                 restart-1                     |      >   restarts_
// |-----------------------------------------------|      |
// |                 restart-n                     |      |
// |-----------------------------------------------|      }                     
// |                 restart-size                  |     <- restarts_.size()
// |-----------------------------------------------|
// 存储方案：
//  1.key存储不同部分进行空间节省；而value部分不进行压缩
//  2.block按group存储（也就是重启点restart），同一个group内的key跟本group上一个key对比，只存储差异的部分
// An entry for a particular key-value pair has the form: （布局如下）
//     shared_bytes: varint32    (跟本group上一个key相同的部分的长度)
//     unshared_bytes: varint32  (跟本group上一个key不同的部分的长度)
//     value_length: varint32    (value值的长度，value不压缩)
//     key_delta: char[unshared_bytes] （value不同部分的数据内容）
//     value: char[value_length] （value值具体的数据内容）
// shared_bytes == 0 for restart points.  （对于restart point， 不压缩，所以shared_bytes == 0）
//
// The trailer of the block has the form: (block的尾部布局如下）
//     restarts: uint32[num_restarts]  （restart数组，是一个offset，指向group的开始地址）
//     num_restarts: uint32 （restart的数量）
// restarts[i] contains the offset within the block of the ith restart point.


// kv对的存储方式如下：
// |-----------------------------------------------|
// |                 shared(4B)                    |
// |-----------------------------------------------|
// |                non_shared(4B)                 | 
// |-----------------------------------------------|
// |                value.size(4B)                 | 
// |-----------------------------------------------|
// |                key的非共享部分字符串            | 
// |-----------------------------------------------|
// |                value整个字符串                 | 
// |-----------------------------------------------|

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;     // 选项，比如重启点options_->block_restart_interval等
  std::string           buffer_;      // Destination buffer  block序列化之后的数据
  std::vector<uint32_t> restarts_;    // Restart points 重启点数组
  int                   counter_;     // Number of entries emitted since restart  (当前)重启点后的entry(即key-value)的数量
  bool                  finished_;    // Has Finish() been called?
  std::string           last_key_;    // 上一次的key

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
