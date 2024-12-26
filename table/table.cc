// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

// Rep的声明放到.cc文件中，而没有放到.h文件，可减少编译依赖
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  // 状态
  Status status;
  // 对应的文件
  RandomAccessFile* file; 
  // 对应的cahce_id
  uint64_t cache_id;
  // filter block的读取器
  FilterBlockReader* filter;
  // fiter数据
  const char* filter_data;
  // 指向metaindex的位置
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  // index_block数据（只有一个index_block）
  Block* index_block;
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) { // Footer可以填充padding，固定是48个字节
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 读取开始位置为：size - Footer::kEncodedLength， 读取Footer::kEncodedLength=48个字节
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength, 
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  // 解释Footer数据
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    // 读取唯一的index block
    s = ReadBlock(file, ReadOptions(), footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

// 1.根据footer(footer的mataindex_handle),读取metaindex block
// 2.在metaindex block查找fiter.xxxx meta的位置
// 3.根据fiter.xxxx meta的位置,读取对应的meta block(主要就是filter block)
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  BlockContents contents;
  // 根据metaindex_handle读取metaindex block
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  // 从metaindex block中查找filer类型meta block对应的metaindex条目
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    // 根据上面查找的位置读取对应的filter block数据
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

// 根据Filter block的位置信息读取对应的fitler block数据，构建一个FilterBlockReader放到rep_->filter
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 通过index的迭代器的值(Block Handle)，获取指向对应block的迭代器
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      // block_cache缓存的key为：cache_id(8B) +  block的offset
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        // 命中缓存从缓存中获取block
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        // 通过block的handle从table文件中读取对应的block
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            // 将block插入block cache中
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  // 构建index block的迭代器
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  // 从table的众多index block中查找，值是一个block handler指向对应的data block
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      // 创建data block的迭代器： 通过对应data block的index
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      // 在data block中查找对应key
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        // 传入的调用hook函数
        (*saver)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();

      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
