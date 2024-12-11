// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    // 计算32KB的block剩余的空间
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // 如果剩余空间不能够Header,将剩余空间填充为0
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        // 剩余部分填充为0(最多7个,根据leftover的大小写)
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // 新开block,重置offset
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 计算出去头部分,能够写数据的大小
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 计算当前block需要写入的大小(考虑分段写的问题)
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 如果剩余要写的大小等于分段写入的大小，说明可以写完
    const bool end = (left == fragment_length);
    // 开始写 并写 写完了，也就是一次性写完
    if (begin && end) {
      type = kFullType;
    // 分段写，写第一个fragment
    } else if (begin) {
      type = kFirstType;
    // 分段写，写最后一个fragment
    } else if (end) {
      type = kLastType;
    // 分段写，写中间的fragment
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  // checksum (4 bytes), length (2 bytes), type (1 byte). see log_format.h
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize)); // 写header
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n)); //写payload
    if (s.ok()) {
      //TODO WAL file 每次立刻flush?
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
