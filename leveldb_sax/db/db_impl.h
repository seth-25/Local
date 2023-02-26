// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"

#include "ST_Compaction.h"
#include "ST_merge.h"
#include "globals.h"
#include "mem_version_set.h"
#include "boost/thread/shared_mutex.hpp"
#include "query_heap.h"
#include "threadPool_2.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname, const void* db_jvm);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, LeafTimeKey& key) override;
  Status Init(LeafTimeKey* leafKeys, int leafKeysNum) override;
  Status RebalanceDranges() override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options,LeafTimeKey& key, int memId) override;
  Status Get(const aquery &aquery1, bool is_use_am, int am_version_id, int st_version_id, jniVector<uint64_t>& st_number,
             jniVector<ares>& results, int& res_amid, jniInfo jniInfo_) override;
  Status Get_exact(const aquery &aquery1, int am_version_id, int st_version_id, jniVector<uint64_t> &st_number,
                   jniVector<ares> &appro_res, jniVector<ares_exact>& results, int appro_am_id,
                   jniVector<uint64_t> &appro_st_number, jniInfo jniInfo_) override;
  void Get_am(const aquery &aquery1, query_heap *res_heap, MemTable* to_find_mem);
  void Get_st(const aquery &aquery1, query_heap *res_heap, uint64_t st_number, Version* this_ver);
  void Get_am_exact(const aquery &aquery1, query_heap_exact *res_heap, MemTable* to_find_mem, bool isappro);
  void Get_st_exact(const aquery &aquery1, query_heap_exact *res_heap, uint64_t st_number, Version* this_ver, bool isappro);
  void UnRef_am(int unref_version_id) override;
  void UnRef_st(int unref_version_id) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };


  //重平衡
  void RebalanceDranges(vector<int>& table_rebalanced);

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable(std::pair<MemTable*, int> aim) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */
      , int memId);
  WriteBatch* BuildBatchGroup(Writer** last_writer, int memId)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  static void BGWork_IM(void* db, std::pair<MemTable*, int> aim);
  static void BGWork_Get_am(void* db, const aquery &aquery1, query_heap *res_heap, MemTable* to_find_mem);
  static void BGWork_Get_st(void* db, const aquery &aquery1, query_heap *res_heap, uint64_t st_number, Version* this_ver);
  static void BGWork_Get_am_exact(void* db, const aquery &aquery1, query_heap_exact *res_heap, MemTable* to_find_mem, bool isappro);
  static void BGWork_Get_st_exact(void* db, const aquery &aquery1, query_heap_exact *res_heap, uint64_t st_number, Version* this_ver, bool isappro);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
//  port::Mutex mutex_;
//  std::atomic<bool> shutting_down_;
//  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
//  MemTable* mem_;
//  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
//  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
//  WritableFile* logfile_;
//  uint64_t logfile_number_ GUARDED_BY(mutex_);
//  log::Writer* log_;
//  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.



  const void* db_jvm;
  //磁盘版本锁
  port::Mutex mutex_;
  //内存版本锁
  port::Mutex mutex_Mem;
  //要删除的表数组
  mems_todel memsTodel;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  //写锁
  vector<port::Mutex> write_mutex;
  //选表锁
  boost::shared_mutex range_mutex;
  //边界
  vector<saxt_only> bounds;
  //内存版本
  mem_version_set* memSet;
  //表
  vector<MemTable*> mems;
  //mem中现有的key数量
  vector<int> memNum;
  //mem中统计一段时间内的插入数量
  vector<int> memNum_period;
  //写队列
  vector<vector<LeafTimeKey>> writers_vec[2];
  int towrite[10];
  vector<port::CondVar> write_signal_;
  bool writers_is[10];
  // im队列
  ThreadPool* pool;
  ThreadPool* pool_get;
  ThreadPool* pool_compaction;
  ThreadPool* pool_snap;
  ST_Compaction_Leaf* stCompactionLeaf;
  bool imms_isdoing[10];
  std::deque<std::pair<MemTable*, int>> imms GUARDED_BY(mutex_);
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ ;
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
//  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
//  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_ GUARDED_BY(mutex_);
  // 当前磁盘版本号
  int versionid;
  unordered_map<int, Version*> version_map;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ ;

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
