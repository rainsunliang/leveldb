// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;          // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    // index block因为相邻的数据共性不大，没有必要做去重存储优化，重启点间隔默认设置为1
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 记录index block（上一block数据刚刷入盘会设置pending_index_entry=true），也就是每个datablock 记录一条index block
  // Tips：一个table只有1个index block, 但是有多个data block和filter block
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 一个新的block,last_key不一定就等于当前key,而是使用了上一个block最后的key和当前key之间的一个字符串（这里叫做"最短分割符"）
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // 记录文件当前的offset和size到handle_encoding
    r->pending_handle.EncodeTo(&handle_encoding);
    // 写index block，其中key为last_key, value为data block(上一个)在文件的offset和size
    // 因为index_block的block_restart_interval默认为1，index_block.Add不会根据key的共享部分和非共享部分分别存储的策略
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 记录filter block
  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  // 写data block
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // 超过block阈值，需要开启新的block
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  // 将data block写入文件，同时将datablock在文件中的offset和data block的size记录到pending_handle
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    // 执行文件flush到磁盘
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 增加restart增加到buffer_，并通过函数返回整个buffer_内容
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

// handle参数可以获取到，当年前写入到文件中的offset和需要写入的文件大小
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  // 先将block的内容增加到文件
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    // 1个字节的压缩类型
    trailer[0] = type;
    // 计算数据部分的crc
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    // crc也算上压缩类型这个字段
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    // tailer最后4个字节写入crc
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    // 将block的尾部（压缩类型+crc）增加到文件-- 到此整个datablock已经写入文件
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 更新整个文件的offet
      r->offset += block_contents.size() + kBlockTrailerSize;
    }  
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

// 进行表Finish的时候
// 1.刷入最后data block到文件（data block是每个block都会写入表的，其他的block是最后Finish的时候真正写入文件）
// 2.写filter block到文件(多个。Tips: filter block是metablock的一种类型)
// 3.写metaindex block(1个)
// 4.写indx block(1个)
// 5.写表表尾部（footer）
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 1.刷入最后的data block到文件
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block 
  // 2.写所有的filter block到文件
  if (ok() && r->filter_block != NULL) {
    // 2.1 r->filter_block->Finish()执行最后一个filter block写入，返回所有的filter block数据
    // 2.2 执行WriteRawBlock，将filter block写入到文件; 同时将filter block在文件中的offset和size记录到了filter_block_handle中
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 3.写metaindex block （当前更像是filter block的指针）
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);

    // 3.1 写策略名：比如 "filter.leveldb.BuiltinBloomFilter"
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      // key 为"filter.leveldb.BuiltinBloomFilter", value为在filter block的offset和size
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    // 写metaindex block, 同时将metaindex block在文件中的offset和size记录到了metaindex_block_handle中
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 4.写index block
  if (ok()) {
    // 4.1 写上一个data block对应的一条index记录
    if (r->pending_index_entry) {
      // last_key的后续key, eg: abc -> abd
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      // value为datablock对应的handle
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    // 4.2 写index block到文件(紧接着metaindex block)，同时index block在在文件中的offset和size记录到了index_block_handle
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 5.写表表尾部（footer）,由两部分组成：1.metaindex block的位置和大小信息 2.index block的位置和大小信息
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    // 将foolter增加都文件尾部
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
