// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// SST的一些属性
struct FileMetaData {
  int refs;         // 当前引用数
  int allowed_seeks;          // Seeks allowed until compaction 允许的无效seek次数,消耗完触发compaction
  uint64_t number;            // 文件编号，唯一识别一个文件
  uint64_t file_size;         // File size in bytes  SST文件的大小
  InternalKey smallest;       // Smallest internal key served by table  最小key
  InternalKey largest;        // Largest internal key served by table   最大key

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  // key 为compact时候的key范围的最大值
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int/*level*/, uint64_t/*file number*/> > DeletedFileSet;

  // 比较器名称
  std::string comparator_;
  // 日志编号，该编号之前的数据可以删除
  uint64_t log_number_;
  // 已经废弃
  uint64_t prev_log_number_;
  // 下一个文件编号（ldb/idb/mainfest共享一个编号空间）
  uint64_t next_file_number_;
  // 数据库已经持久化数据项中最大的squence number
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  // compaction记录信息,每个层级compact的最大key(下一次同样的level compact可以从这个key后面开始compact)
  std::vector< std::pair<int/*level*/, InternalKey> > compact_pointers_;
  // 相对上一个version,需要删除的文件列表
  DeletedFileSet deleted_files_;
  // 相对上一个version,新增的文件列表
  std::vector< std::pair<int/*level*/, FileMetaData/*SST*/> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
