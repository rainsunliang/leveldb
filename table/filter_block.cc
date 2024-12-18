// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

// 先建一个filter block,
// 注意：所有的filter block因为是最后统一写到data block后面的，
// 所以filter block的序列化数据会攒起来到filterblock的result_中
void FilterBlockBuilder::StartBlock(uint64_t block_offset) { 
  // 计算当前的filter下标
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  // 当前的filter下标 大于 已经存在的filter，则逐个补充创建
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

// 将当前key暂存到keys_中,后续在FilterBlockBuilder::Finish的时候会
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  // key在keys开始的位置
  start_.push_back(keys_.size());
  // 所有key无缝拼接到keys_中
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  // 当前有key则进入创建filter数据流程
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    // 将每个filter在result_中的开始位置也写入到result_
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 写入所有fitler总的偏移到result_
  PutFixed32(&result_, array_offset);
  // 写入参数filterbase到result_
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

// 为当前集合keys_所有的key创建fiter数据放到result_中
void FilterBlockBuilder::GenerateFilter() {
  // 获取当前key的数量
  const size_t num_keys = start_.size();

  // 如果当前没有key
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    // 因为result_和filter_offsets_是累计的
    filter_offsets_.push_back(result_.size());
    return;
  }

  // 如果当前有key，则走下面流程

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  // tmp_keys_扩展到num_keys字节，准备从key_和start_还原出数据放到tmp_keys中
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 记录新的fiter在result_的开始位置
  filter_offsets_.push_back(result_.size());
  // 为当前集合keys_所有的key构建一个fiter数据，并写入到result_中
  policy_->CreateFilter(&tmp_keys_[0], num_keys, &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  // 读取baselg
  base_lg_ = contents[n-1];
  // 读取fitleroffset的数量（每个filter-data每2^base_lg_ KB数据对应一个filter)
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  // offset_指向第1个filter offset的位置
  offset_ = data_ + last_word;
  // filter offset数量 = （block总大小n - 1字节baselg - 4字节 fillter offset 开始位置）/ 4字节 每个一个 fillter offset 
  num_ = (n - 5 - last_word) / 4;
}

// 
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // 通过block_offset计算落到哪个filter中（因为每2^base_lg_ KB data block数据，对应一个fiter）
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 计算对应filter的开始和结束位置
    uint32_t start = DecodeFixed32(offset_ + index*4);
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= (offset_ - data_)) {
      // 从filter block的data_中提取对应filter数据
      Slice filter = Slice(data_ + start, limit - start);
      // 判断对应的key是否在filter中有命中
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
