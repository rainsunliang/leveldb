// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log { class Writer; }

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==NULL represents a key smaller than all keys in the DB.
// largest==NULL represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
extern bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // 创建每一层的迭代器，其中第0层和非0层的迭代器不一样：
  // 第0存在key重叠；全部读入cache缓存；
  // 大于0层不存在key重叠,使用NewConcatenatingIterator,惰性读取；
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };
  // 从SST读取指定key的数据
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  // 当查找文件miss则增加seek次数
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  // 获取指定层，与[begin, end]有重叠的SST列表
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,         // NULL means before all keys
      const InternalKey* end,           // NULL means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  // 判断[smallest_user_key，largest_user_key]与指定层是否有重叠
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  // 选择内存table要写到哪一层
  // dump 0层的时候，尽量选择一个level大的层，机制如下：
  // 1. 如果放到对应的层数会导致文件间不严格有序，会影响读取，则不再尝试。
  // 2. 如果放到 level + 1层，与 level + 2层的文件重叠很大，就会导致 compact 到该文件时，overlap文件过大，则不再尝试。
  // 3. 最大返回 level 2
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  // 返回一个NewTwoLevelIterator
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  // 查找key区间与user_key有重合的SST集合，逐个调用func函数
  void ForEachOverlapping(Slice user_key, Slice internal_key,
                          void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;            // VersionSet to which this Version belongs 属于哪个versionset
  Version* next_;               // Next version in linked list
  Version* prev_;               // Previous version in linked list
  int refs_;                    // Number of live refs to this version 当前version的引用数

  // List of files per level 每层的文件列表
  std::vector<FileMetaData*> files_[config::kNumLevels]; 

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_; // seek次数超过阈值后需要压缩的文件
  int file_to_compact_level_;     // seek次数超过阈值后需要压缩的文件所在的层数

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_; // 用于检查size超过阈值后需要压缩的文件
  int compaction_level_;    // 用于检查size超过阈值后需要压缩的文件所在的层数

  explicit Version(VersionSet* vset)
      : vset_(vset), next_(this), prev_(this), refs_(0),
        file_to_compact_(NULL),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {
  }

  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             TableCache* table_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  // 从持久化中恢复
  Status Recover();

  // Return the current version.
  // 返回当前version
  Version* current() const { return current_; }

  // Return the current manifest file number
  // 返回当前manifest文件编号
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  // 分配新的文件编号
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  // 复用文件号: 从file_number+1开始复用？
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  // 选择参与压缩的level和文件
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  // 获取制定level层，[begin, end]可以压缩的文件
  Compaction* CompactRange(
      int level,
      const InternalKey* begin,
      const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  // 获取level+1层重叠部分的字节数
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  // 为参与压缩的文件创建一个迭代器
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  // 判断是否需要压缩
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != NULL);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  // 获取该key近似的偏移量
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest,
                 InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  // 增加一个版本
  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  // 下一个文件编号
  uint64_t next_file_number_;
  // manifest文件编号
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  // 当前日志编号
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  // 写manifest的fd
  WritableFile* descriptor_file_;
  // 写日志的fd
  log::Writer* descriptor_log_;
  // 双向循环脸链表的表头
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  // 最新版本（dummy_versions_的pre指current）
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  explicit Compaction(int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

  // State used to check for number of of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
