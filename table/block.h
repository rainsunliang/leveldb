// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

// Block的布局
// |-----------------------------------------------|    <- data_              }
// |                 k-v-1                         |                          |
// |-----------------------------------------------|                          |
// |                 k-v-2                         |                          |
// |-----------------------------------------------|                          |  
// |                 k-v-n                         |                          |  
// |-----------------------------------------------|    <- restart_offset      >  size
// |                 restart-0                     |                          |
// |-----------------------------------------------|                          |
// |                 restart-1                     |                          |
// |-----------------------------------------------|                          |
// |                 restart-n                     |                          |
// |-----------------------------------------------|                          |
// |                 restart-size                  |                          |  
// |-----------------------------------------------|                          }
// tips: block 在Table::BlockReader被读取的时候会被插入缓存中
class Block {
 public:
  // Initialize the block with the specified contents.
  /**
   * 从BlockContents中解析出对应的kv数据和restart数据，
   * 并提供迭代去进行kv数据的遍历操作
   */
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  // block有很多kv对，需要提供迭代器遍历 (在Table::BlockReader函数中调用)
  Iterator* NewIterator(const Comparator* comparator);

 private:
  uint32_t NumRestarts() const;

  // 整个block数据
  const char* data_;
  // 整个block数据的大小
  size_t size_;

  // 重启点数组在data_的开始位置
  uint32_t restart_offset_;     // Offset in data_ of restart array
  bool owned_;                  // Block owns data_[]

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);

  class Iter;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
