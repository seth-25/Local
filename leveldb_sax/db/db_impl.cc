// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <set>
#include <string>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"


#include "send_master.h"
#include "thread"
#include "zsbtree/newVector.h"
#include "zsbtree_finder.h"
#include "ZsbTree_finder_exact.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
    ts_time startTime, endTime;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname, const void* db_jvm)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      versionid(0),
      memSet(new mem_version_set()),
      db_jvm(db_jvm),
//      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {
  pool = new ThreadPool(pool_size);
  pool_get = new ThreadPool(pool_get_size);
  pool_compaction = new ThreadPool(pool_compaction_size);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }

  for (auto item: version_map) {
    item.second->Unref();
  }

  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }


  delete versions_;
//  if (imm_ != nullptr) imm_->Unref();


  mutex_Mem.Lock();
  memSet->UnrefAll();
  delete memSet;
  mutex_Mem.Unlock();


  delete_mems(memsTodel.get());

  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }

}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        out("Delete type="+to_string(type)+" "+to_string(number));
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }

  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  //log不要了
//  std::sort(logs.begin(), logs.end());
//  for (size_t i = 0; i < logs.size(); i++) {
//    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
//                       &max_sequence);
//    if (!s.ok()) {
//      return s;
//    }
//    out(4);
//    // The previous incarnation may not have written any MANIFEST
//    // records after allocating this log number.  So we manually
//    // update the file number allocation counter in VersionSet.
//    versions_->MarkFileNumberUsed(logs[i]);
//  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {

//  struct LogReporter : public log::Reader::Reporter {
//    Env* env;
//    Logger* info_log;
//    const char* fname;
//    Status* status;  // null if options_.paranoid_checks==false
//    void Corruption(size_t bytes, const Status& s) override {
//      Log(info_log, "%s%s: dropping %d bytes; %s",
//          (this->status == nullptr ? "(ignoring error) " : ""), fname,
//          static_cast<int>(bytes), s.ToString().c_str());
//      if (this->status != nullptr && this->status->ok()) *this->status = s;
//    }
//  };
//
//  mutex_.AssertHeld();
//
//  // Open the log file
//  std::string fname = LogFileName(dbname_, log_number);
//  SequentialFile* file;
//  Status status = env_->NewSequentialFile(fname, &file);
//  if (!status.ok()) {
//    MaybeIgnoreError(&status);
//    return status;
//  }
//
//  // Create the log reader.
//  LogReporter reporter;
//  reporter.env = env_;
//  reporter.info_log = options_.info_log;
//  reporter.fname = fname.c_str();
//  reporter.status = (options_.paranoid_checks ? &status : nullptr);
//  // We intentionally make log::Reader do checksumming even if
//  // paranoid_checks==false so that corruptions cause entire commits
//  // to be skipped instead of propagating bad information (like overly
//  // large sequence numbers).
//  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
//  Log(options_.info_log, "Recovering log #%llu",
//      (unsigned long long)log_number);
//
//  // Read all the records and add to a memtable
//  std::string scratch;
//  Slice record;
//  WriteBatch batch;
//  int compactions = 0;
//  MemTable* mem = nullptr;
//  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
//    if (record.size() < 12) {
//      reporter.Corruption(record.size(),
//                          Status::Corruption("log record too small"));
//      continue;
//    }
//    WriteBatchInternal::SetContents(&batch, record);
//
//    if (mem == nullptr) {
//      mem = new MemTable(internal_comparator_);
//      mem->Ref();
//    }
//    status = WriteBatchInternal::InsertInto(&batch, mem);
//    MaybeIgnoreError(&status);
//    if (!status.ok()) {
//      break;
//    }
//    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
//                                    WriteBatchInternal::Count(&batch) - 1;
//    if (last_seq > *max_sequence) {
//      *max_sequence = last_seq;
//    }
//
//    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
//      compactions++;
//      *save_manifest = true;
//      status = WriteLevel0Table(mem, edit, nullptr);
//      mem->Unref();
//      mem = nullptr;
//      if (!status.ok()) {
//        // Reflect errors immediately so that conditions like full
//        // file-systems cause the DB::Open() to fail.
//        break;
//      }
//    }
//  }
//
//  delete file;
//
//  // See if we should keep reusing the last log file.
//  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
//    assert(logfile_ == nullptr);
//    assert(log_ == nullptr);
//    assert(mem_ == nullptr);
//    uint64_t lfile_size;
//    if (env_->GetFileSize(fname, &lfile_size).ok() &&
//        env_->NewAppendableFile(fname, &logfile_).ok()) {
//      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
//      log_ = new log::Writer(logfile_, lfile_size);
//      logfile_number_ = log_number;
//      if (mem != nullptr) {
//        mem_ = mem;
//        mem = nullptr;
//      } else {
//        // mem can be nullptr if lognum exists but was empty.
//        mem_ = new MemTable(internal_comparator_);
//        mem_->Ref();
//      }
//    }
//  }
//
//  if (mem != nullptr) {
//    // mem did not get reused; compact it.
//    if (status.ok()) {
//      *save_manifest = true;
//      status = WriteLevel0Table(mem, edit, nullptr);
//    }
//    mem->Unref();
//  }
//
//  return status;

}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  //这里 InternalKey 先放着，后面要去掉顺序号再改
  FileMetaData meta;

  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);

  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, mem, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());

  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    Version* base = versions_->current();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    out("压缩st0==================\nfilenumber:"+to_string(meta.number)
        +"\nfile_size:"+to_string(meta.file_size) + "\nlevel:" + to_string(level)
        +"\n最小最大:");
    saxt_print(min_user_key.data());
    saxt_print(max_user_key.data());
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest, meta.startTime, meta.endTime);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable(std::pair<MemTable*, int> aim) {

  mutex_.Lock();
//  assert(imm_ != nullptr);
  MemTable* first_imm = aim.first;

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
//  Version* base = versions_->current();
//  base->Ref();
  Status s = WriteLevel0Table(first_imm, &edit);
//  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
//    imm_->Unref();
    //todo
    //在这里应该要发消息给master
    //发version的版本号，当前的版本号，加上version edit,不过edit的删除文件要有元文件信息
    //还要发当前内存的current的版本号
    mutex_Mem.Lock();
    int memId = aim.second;
    int amV_id = memSet->newversion(mems[memId], memId);
    mems[memId]->Unref();
//    for(auto item:mems) assert(item->refs_>0);
    mutex_Mem.Unlock();
    imms_isdoing[memId] = false;

    Version* nowversion = versions_->current();
    //这个ref只能master来解除
    nowversion->Ref();
    version_map[++versionid] = nowversion;
    out(versionid);
    // 发送 versionid, amV_id, edit.new_files_ 3样东西
    char* to_send = (char*)malloc(send_size1);
    char* tmp_to_send = to_send;
    tmp_to_send[0] = 0;
    tmp_to_send++;
    charcpy(tmp_to_send, &versionid, sizeof(int));
    charcpy(tmp_to_send, &amV_id, sizeof(int));
    FileMetaData& metaData = edit.new_files_[0].second;
    charcpy(tmp_to_send, &metaData.number, sizeof(uint64_t));
    charcpy(tmp_to_send, metaData.smallest.user_key().data(), sizeof(saxt_only));
    charcpy(tmp_to_send, metaData.largest.user_key().data(), sizeof(saxt_only));
    charcpy(tmp_to_send, &metaData.startTime, sizeof(ts_time));
    charcpy(tmp_to_send, &metaData.endTime, sizeof(ts_time));
    send_master(to_send, send_size1, db_jvm);
    //等待返回接到回复
    free(to_send);
    //
//    imm_ = nullptr;
    RemoveObsoleteFiles();
    VersionSet::LevelSummaryStorage tmp;
    out("im压缩文件号："+to_string(metaData.number)+" compacted to: "+ (string)versions_->LevelSummary(&tmp));
//    cout<<"im压缩文件号："+to_string(metaData.number)+" compacted to: "+ (string)versions_->LevelSummary(&tmp)<<endl;
  } else {

    RecordBackgroundError(s);
  }
  // 如果有新的表要插入，要唤醒
  background_work_finished_signal_.SignalAll();
  mutex_.Unlock();
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
//  Status s = Write(WriteOptions(), nullptr, 0);
//  if (s.ok()) {
//    // Wait until the compaction completes
//    MutexLock l(&mutex_);
//    while (!imms.empty() && bg_error_.ok()) {
//      background_work_finished_signal_.Wait();
//    }
//    if (!imms.empty()) {
//      s = bg_error_;
//    }
//  }
//  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imms.empty() && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BGWork_IM(void* db, std::pair<MemTable*, int> aim) {
  reinterpret_cast<DBImpl*>(db)->CompactMemTable(aim);
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();
  //同一时间只有一个线程能运行
  if (!imms.empty()) {
    pool->enqueue(&DBImpl::BGWork_IM, this, imms.front());
//    out("success im compaction");
    imms.pop_front();
    if (imms.empty()) has_imm_.store(false, std::memory_order_release);
    return;
  }
  out("进入压缩合并");
  //压缩合并sstable，这里写完读取sstable再来写
  Compaction* c;
  // 手动合并，测试用的
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }
//  out("Pick");

  Status status;
  if (c == nullptr) {
    // Nothing to do
  }
  else if (!is_manual && c->IsTrivialMove()) {
    //下一层没有与这个文件范围重叠的，直接移动
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    out("直接下移");
    out(f->number);
    out(c->level());
    out(f->file_size);
    saxt_print(f->smallest.user_key().data());
    saxt_print(f->largest.user_key().data());
    c->edit()->RemoveFile(c->level(), f->number, f->file_size, f->smallest,
                          f->largest, f->startTime, f->endTime);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest, f->startTime, f->endTime);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {

      RecordBackgroundError(status);
    }
    //版本号没变，不用上报
    Version* newversion = versions_->current();
    newversion->Ref();
    version_map[versionid]->Unref();
    version_map[versionid] = newversion;

    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    out(" 进入多文件合并");
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      out(" DoCompactionWork错了");

      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);
//  out("FinishCompactionOutputFile");
  // Check for iterator errors
  Status s;
//  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->current_output()->startTime = compact->compaction->startTime;
  compact->current_output()->endTime = compact->compaction->endTime;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  //缓存测试
//  if (s.ok() && current_entries > 0) {
//    // Verify that the table is usable
//    Iterator* iter =
//        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
//    s = iter->status();
//    delete iter;
//    if (s.ok()) {
//      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
//          (unsigned long long)output_number, compact->compaction->level(),
//          (unsigned long long)current_entries,
//          (unsigned long long)current_bytes);
//    }
//  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));
//  printf("Compacted %d@%d + %d@%d files => %lld bytes\n",compact->compaction->num_input_files(0), compact->compaction->level(),
//         compact->compaction->num_input_files(1), compact->compaction->level() + 1,
//         static_cast<long long>(compact->total_bytes));
  // Add compaction outputs
  VersionEdit* edit = compact->compaction->edit();
  compact->compaction->AddInputDeletions(edit);
  const int level = compact->compaction->level();
//  cout<<"输入"<<compact->compaction->num_input_files(0)+compact->compaction->num_input_files(1)<<endl;
//  cout<<"输出"<<compact->outputs.size()<<endl;
  for (auto & out : compact->outputs) {
    out("压缩合并输出一个文件");
    out(out.number);
    out(out.file_size);
    out(level);
    out("最小最大");
    saxt_print((saxt)out.smallest.user_key().data());
    saxt_print((saxt)out.largest.user_key().data());
    edit->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest, out.startTime, out.endTime);
  }

  Status res = versions_->LogAndApply(edit, &mutex_);
  //改变版本
  //todo
  //在这里应该要发消息给master
  //发version的版本号，当前的版本号，加上version edit,不过edit的删除文件要有元文件信息
  //还要发当前内存的current的版本号
  Version* nowversion = versions_->current();
  //这个ref只能master来解除
  nowversion->Ref();
  version_map[++versionid] = nowversion;
  // 发送 versionid, edit->new_files_ edit->deleted_files_Meta 4样东西
  int delsize = edit->deleted_files_Meta.size();
  int addsize = edit->new_files_.size();
  size_t mallocsize = send_size2+delsize*send_size2_add+addsize*send_size2_add;
  char* to_send = (char*)malloc(mallocsize);
  char* tmp_to_send = to_send;
  tmp_to_send[0] = 1;
  tmp_to_send++;
  charcpy(tmp_to_send, &versionid, sizeof(int));
  charcpy(tmp_to_send, &delsize, sizeof(int));
  for(auto metaData: edit->deleted_files_Meta) {
    charcpy(tmp_to_send, &metaData.number, sizeof(uint64_t));
    charcpy(tmp_to_send, metaData.smallest.user_key().data(), sizeof(saxt_only));
    charcpy(tmp_to_send, metaData.largest.user_key().data(), sizeof(saxt_only));
    charcpy(tmp_to_send, &metaData.startTime, sizeof(ts_time));
    charcpy(tmp_to_send, &metaData.endTime, sizeof(ts_time));
  }
  charcpy(tmp_to_send, &addsize, sizeof(int));
  for(auto item: edit->new_files_) {
    FileMetaData& metaData = item.second;
    charcpy(tmp_to_send, &metaData.number, sizeof(uint64_t));
    charcpy(tmp_to_send, metaData.smallest.user_key().data(), sizeof(saxt_only));
    charcpy(tmp_to_send, metaData.largest.user_key().data(), sizeof(saxt_only));
    charcpy(tmp_to_send, &metaData.startTime, sizeof(ts_time));
    charcpy(tmp_to_send, &metaData.endTime, sizeof(ts_time));
  }
  send_master(to_send, mallocsize, db_jvm);
  free(to_send);
  //等待返回接到回复
  //

  return res;
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
//  cout<<"压缩合并"<<compact->compaction->num_input_files(0)+compact->compaction->num_input_files(1)<<endl;

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  out("Merge");
//  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  ST_merge stMerge(versions_, compact->compaction, pool_compaction);
  out("Merge Create finish");
  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

//  input->SeekToFirst();
  Status status;
//  ParsedInternalKey ikey;
//  std::string current_user_key;
//  bool has_current_user_key = false;
//  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  LeafKey leafKey;
  LeafKey oldKey;
  Zsbtree_Build* zsbtreeBuild;
  //因为两个文件范围不能重合，除了最后一个文件
  bool tocompact_flag = false;
  //调试用
//  int k=0;
  while (stMerge.next(leafKey)) {
//    saxt_print(leafKey.asaxt);
    //调试用
//    assert(!k ||  leafKey >= oldKey);
//    if (k && leafKey < oldKey) {
//      out(k);
//      saxt_print(leafKey.asaxt);
//      saxt_print(oldKey.asaxt);
//      exit(1);
//    }
//    k++;
//    oldKey.Set(leafKey);


    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
//      out("进入im");
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (!imms.empty()) {
        pool->enqueue(&DBImpl::BGWork_IM, this, imms.front());
        imms.pop_front();
        if (imms.empty()) has_imm_.store(false, std::memory_order_release);
        // Wake up MakeRoomForWrite() if necessary.
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    /* 一百年用不上，但是耗时20%
    //internalkey
    InternalKey ikey(Slice((char*)leafKey.asaxt, sizeof(saxt_only)), 0, static_cast<ValueType>(0));
    Slice key = ikey.Encode();
    //和level+2中很多文件重合了
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
//      out("重合了");
//      out("largest");
//      saxt_print(zsbtreeBuild->GetLastLeafKey()->asaxt);
      InternalKey ikey1(Slice((char*)(zsbtreeBuild->GetLastLeafKey()->asaxt), sizeof(saxt_only)), 0, static_cast<ValueType>(0));
      Slice key1 = ikey1.Encode();
      compact->current_output()->largest.DecodeFrom(key1);
      zsbtreeBuild->finish();
      compact->builder->AddRootKey(zsbtreeBuild->GetRootKey());
      assert(compare_saxt(zsbtreeBuild->GetRootKey()->rsaxt, (saxt)compact->current_output()->largest.user_key().data()));
      delete zsbtreeBuild;
      status = FinishCompactionOutputFile(compact);
      if (!status.ok()) {
        break;
      }
    }
     */

    if (tocompact_flag && leafKey > oldKey) {

      out("压缩文件");
//      out("largest");
//      saxt_print(oldKey.asaxt.asaxt);
//      saxt_print(leafKey.asaxt.asaxt);
      zsbtreeBuild->finish();
      compact->builder->AddRootKey(zsbtreeBuild->GetRootKey());

      assert(zsbtreeBuild->GetRootKey()->rsaxt == saxt_only(compact->current_output()->largest.user_key().data()));
      delete zsbtreeBuild;
      status = FinishCompactionOutputFile(compact);
      if (!status.ok()) {
        break;
      }
      tocompact_flag = false;
    }

#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
    // Open output file if necessary
    if (compact->builder == nullptr) {
//      out("开始创建放入");
      status = OpenCompactionOutputFile(compact);
      if (!status.ok()) {
        break;
      }
//      out("small");
//      saxt_print(leafKey.asaxt);
      zsbtreeBuild = new ST_Conmpaction(Leaf_maxnum, Leaf_minnum, compact->builder);
      InternalKey ikey(Slice((char*)leafKey.asaxt.asaxt, sizeof(saxt_only)), 0, static_cast<ValueType>(0));
      Slice key = ikey.Encode();
      compact->current_output()->smallest.DecodeFrom(key);
    }
//    if (!compact->builder->FileSize()) {
//      compact->current_output()->smallest.DecodeFrom(key);
//    }
//    out("add前");
    zsbtreeBuild->Add(leafKey);
//    out("add完");
    // Close output file if it is big enough
    if (!tocompact_flag && compact->builder->FileSize() >=
        compact->compaction->MaxOutputFileSize()) {
//      cout<<"max_size"<<compact->compaction->MaxOutputFileSize()<<endl;
      oldKey = leafKey;
      tocompact_flag = true;
      InternalKey ikey(Slice((char*)leafKey.asaxt.asaxt, sizeof(saxt_only)), 0, static_cast<ValueType>(0));
      Slice key = ikey.Encode();
      compact->current_output()->largest.DecodeFrom(key);
    }

  }


//  if (status.ok() ) {
//    out("db关了");
//    status = Status::IOError("Deleting DB during compaction");
//  }

//  out("完成压缩");
  if (status.ok() && compact->builder != nullptr) {
    out("压缩清尾");
//    out("largest");
//    saxt_print(leafKey.asaxt.asaxt);
    InternalKey ikey(Slice((char*)leafKey.asaxt.asaxt, sizeof(saxt_only)), 0, static_cast<ValueType>(0));
    Slice key = ikey.Encode();
    compact->current_output()->largest.DecodeFrom(key);
    zsbtreeBuild->finish();
    out((int)zsbtreeBuild->nonleafkeys.size());
    compact->builder->AddRootKey(zsbtreeBuild->GetRootKey());
    assert(zsbtreeBuild->GetRootKey()->rsaxt == saxt_only(compact->current_output()->largest.user_key().data()));
    delete zsbtreeBuild;
    status = FinishCompactionOutputFile(compact);
  }


  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
//  out("改变version");
  stats_[compact->compaction->level() + 1].Add(stats);
//  out("更改完version");
  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {

    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
//  string a = "compacted to: "+ (string)versions_->LevelSummary(&tmp);
//  cout<<a<<endl;

//  exit(1);
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
//  mutex_.Lock();
//  *latest_snapshot = versions_->LastSequence();
//
//  // Collect together all needed child iterators
//  std::vector<Iterator*> list;
//  list.push_back(mem_->NewIterator());
//  mem_->Ref();
//  if (imm_ != nullptr) {
//    list.push_back(imm_->NewIterator());
//    imm_->Ref();
//  }
//  versions_->current()->AddIterators(options, &list);
//  Iterator* internal_iter =
//      NewMergingIterator(&internal_comparator_, &list[0], list.size());
//  versions_->current()->Ref();
//
//  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
//  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);
//
//  *seed = ++seed_;
//  mutex_.Unlock();
//  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}
/*
Status DBImpl::Get(const ReadOptions& options, const saxt& key, const int memId) {
  Status s;

  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mems[memId];
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}
*/



Status DBImpl::Get(const aquery& aquery1,
                   bool is_use_am, int am_version_id, int st_version_id, jniVector<uint64_t>& st_number,
                   jniVector<ares>& results, int& res_amid, jniInfo jniInfo_) {
//  cout<<"近似"<<endl;
  out("开始查询=========================");
//  saxt_print(aquery1.asaxt);
  out("k:"+to_string(aquery1.k));
#if istime
  out("starttime:"+to_string(aquery1.rep.startTime));
  out("endtime:"+to_string(aquery1.rep.endTime));
#endif
  out1("is_use_am", is_use_am);
  out1("am_version_id", am_version_id);
  out1("st_version_id:", st_version_id);
  out("开始查询000000000000000000000000");

  if (is_use_am) {
    //有am，先查am，因为可以剪枝
    mutex_Mem.Lock();
    mem_version* this_mem_version = memSet->OldVersion(am_version_id);
    this_mem_version->Ref();
    mutex_Mem.Unlock();
    //顺序 选表
    vector<saxt_only> this_bounds = this_mem_version->boundary->Get();
    int i = 1;
    for (; i < mems.size(); i++) {
      if (this_bounds[i] > aquery1.asaxt) break;
    }
    MemTable* this_mem = this_mem_version->mems[i - 1];

#if istime
    bool istrue = !(this_mem->startTime > aquery1.rep.endTime ||
                    this_mem->endTime < aquery1.rep.startTime);

#else
    bool istrue = true;
#endif
    if (istrue) {
      out("时间范围对上");
      res_amid = i - 1;
      //表范围有重合
      query_heap* res_heap = new query_heap(aquery1.k, st_number.size() + 1);
      res_heap->vv1 = this_mem_version;
      res_heap->v1_mutex = &mutex_Mem;
      if (!st_number.empty()) {
        mutex_.Lock();
        version_map[st_version_id]->Ref();
        res_heap->vv2 = version_map[st_version_id];
        res_heap->v2_mutex = &mutex_;
        mutex_.Unlock();
      }

      res_heap->Lock();


      pool_get->enqueue(&DBImpl::BGWork_Get_am, this, aquery1, res_heap,
                        this_mem);
      //      std::thread athread(&DBImpl::BGWork_Get_am, this, aquery1, res_heap, this_mem); athread.detach();

      for (int j=0;j<st_number.size();j++) {
        pool_get->enqueue(&DBImpl::BGWork_Get_st, this, aquery1, res_heap, st_number[j],
                          res_heap->vv2);
        //        std::thread sthread(&DBImpl::BGWork_Get_st, this, aquery1, res_heap, j, res_heap->vv2); sthread.detach();
      }

      //等待结果
      res_heap->wait();

#if cha == 0
      // 按dist排序，分成20份依次查询
      res_heap->sort_dist_p();

      int div = min(Get_div, (int)(res_heap->to_sort_dist_p.size() -1)/ aquery1.k + 1);
      div = max(div, (int)(res_heap->to_sort_dist_p.size() -1)/ info_p_max_size + 1);
      int num_per = (res_heap->to_sort_dist_p.size()-1)/div+1;

//      char* info = (char*)malloc(to_find_size_leafkey + sizeof(void*) * num_per);
      char* info = jniInfo_.info_p;
      char* add_info = info;
      charcpy(add_info, &aquery1.rep, sizeof(aquery_rep));
#if isap
      charcpy(add_info, &res_heap, sizeof(void*));
#endif
      charcpy(add_info, &aquery1.k, sizeof(int));
      bool isover = false;
      for(int ii =0; ii <div; ii++) {
        int num_one = min((ii +1)*num_per, (int)res_heap->to_sort_dist_p.size()) -
            ii *num_per;
        char* tmpinfo = add_info;
        int need = res_heap->need();
        float topdist = res_heap->top();
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &topdist, sizeof(float));
        char* numinfo = tmpinfo;
        tmpinfo += sizeof(int);
        int real_num = 0;
        for(int j= ii *num_per;j< ii *num_per+num_one;j++){
          if (res_heap->to_sort_dist_p[j].first <= topdist)
            charcpy(tmpinfo, &res_heap->to_sort_dist_p[j].second, sizeof(void*)), real_num++;
          else {
            isover = true;
            break;
          }
        }
        charcpy(numinfo, &real_num, sizeof(int));
#if isap
//        find_tskey_ap(info, to_find_size_leafkey + sizeof(void*) * real_num, db_jvm);
        find_tskey_ap_buffer(jniInfo_, db_jvm);
#else
        char* out;
        size_t out_size;
        find_tskey(info, to_find_size_leafkey + sizeof(void*) * real_num, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        //push
        for (int j=0;j<out_size;j++) {
          if (!res_heap->push(ares_out[j])) break;
        }
        free(out);
#endif
        if(isover) break;
      }
      res_heap->get(results);
      res_heap->Unlock();
      delete res_heap;
#else
      res_heap->get(results);
      bool isdel = res_heap->subOver();
      res_heap->Unlock();
      if (isdel) delete res_heap;
#endif
//      free(info);
      return Status();
    } else {
      out("时间范围没对上");
      mutex_Mem.Lock();
      this_mem_version->Unref();
      mutex_Mem.Unlock();
    }
  }



  if (st_number.empty()) return Status();

  query_heap *res_heap = new query_heap(aquery1.k, st_number.size());


  mutex_.Lock();
  version_map[st_version_id]->Ref();
  res_heap->vv2 = version_map[st_version_id];
  res_heap->v2_mutex = &mutex_;
  mutex_.Unlock();

  res_heap->Lock();


  out("st任务");
  for (int j=0;j<st_number.size();j++) {
    pool_get->enqueue(&DBImpl::BGWork_Get_st, this, aquery1, res_heap, st_number[j], res_heap->vv2);
//    std::thread sthread(&DBImpl::BGWork_Get_st, this, aquery1, res_heap, j, res_heap->vv2);
//    sthread.detach();
  }



  res_heap->wait();
#if cha == 0
  // 按dist排序，分成20份依次查询
  res_heap->sort_dist_p();

  int div = min(Get_div, (int)(res_heap->to_sort_dist_p.size() -1)/ aquery1.k + 1);
  div = max(div, (int)(res_heap->to_sort_dist_p.size() -1)/ info_p_max_size + 1);
  int num_per = (res_heap->to_sort_dist_p.size()-1)/div+1;

  char* info = jniInfo_.info_p;
//  char* info = (char*)malloc(to_find_size_leafkey + sizeof(void*) * num_per);
  char* add_info = info;
  charcpy(add_info, &aquery1.rep, sizeof(aquery_rep));
#if isap
  charcpy(add_info, &res_heap, sizeof(void*));
#endif
  charcpy(add_info, &aquery1.k, sizeof(int));
  bool isover = false;
  for(int ii =0; ii <div; ii++) {
    int num_one = min((ii +1)*num_per, (int)res_heap->to_sort_dist_p.size()) -
                  ii *num_per;
    char* tmpinfo = add_info;
    int need = res_heap->need();
    float topdist = res_heap->top();
    charcpy(tmpinfo, &need, sizeof(int));
    charcpy(tmpinfo, &topdist, sizeof(float));
    char* numinfo = tmpinfo;
    tmpinfo += sizeof(int);
    int real_num = 0;
    for(int j= ii *num_per;j< ii *num_per+num_one;j++){
      if (res_heap->to_sort_dist_p[j].first <= topdist)
        charcpy(tmpinfo, &res_heap->to_sort_dist_p[j].second, sizeof(void*)), real_num++;
      else {
        isover = true;
        break;
      }
    }
    charcpy(numinfo, &real_num, sizeof(int));
#if isap
//    find_tskey_ap(info, to_find_size_leafkey + sizeof(void*) * real_num, db_jvm);
    find_tskey_ap_buffer(jniInfo_, db_jvm);
#else
    char* out;
    size_t out_size;
    find_tskey(info, to_find_size_leafkey + sizeof(void*) * real_num, out, out_size, db_jvm);
    ares* ares_out = (ares*) out;
    //push
    for (int j=0;j<out_size;j++) {
      if (!res_heap->push(ares_out[j])) break;
    }
    free(out);
#endif
    if(isover) break;
  }
  res_heap->get(results);
  res_heap->Unlock();
  delete res_heap;
#else
  res_heap->get(results);
  bool isdel = res_heap->subOver();
  res_heap->Unlock();
  if (isdel) delete res_heap;
#endif
//  free(info);
  return Status();
}


void DBImpl::BGWork_Get_am(void* db, const aquery& aquery1,
                           query_heap* res_heap, MemTable* to_find_mem) {
  reinterpret_cast<DBImpl*>(db)->Get_am(aquery1, res_heap, to_find_mem);
}
void DBImpl::BGWork_Get_st(void* db, const aquery& aquery1,
                           query_heap* res_heap, uint64_t st_number,
                           Version* this_ver) {
  reinterpret_cast<DBImpl*>(db)->Get_st(aquery1, res_heap, st_number, this_ver);
}


void DBImpl::Get_am(const aquery& aquery1, query_heap* res_heap,
                    MemTable* to_find_mem) {

  LeafKey* res_leafkeys = (LeafKey*)malloc(sizeof(LeafKey)*Leaf_rebuildnum);
  int res_leafkeys_num;
  auto res_p = (dist_p *)malloc(sizeof(dist_p)*Leaf_rebuildnum);
  int res_p_num;
  char* info = (char*)malloc(to_find_size_leafkey + sizeof(void*) * Leaf_rebuildnum);
  char* add_info = info;
  charcpy(add_info, &aquery1.rep, sizeof(aquery_rep));
  charcpy(add_info, &aquery1.k, sizeof(int));

#if istime == 2
  ZsbTree_finder Finder(aquery1.asaxt, aquery1.rep.startTime, aquery1.rep.endTime, (ts_type*)aquery1.paa);
#else
  ZsbTree_finder Finder(aquery1.asaxt, (ts_type*)aquery1.paa);
#endif
  Finder.root_Get(*(to_find_mem->GetRoot()));

  //必找的一个点
  Finder.find_One(res_leafkeys, res_leafkeys_num);

  for(int i=0;i<res_leafkeys_num;i++) {
    saxt_print(res_leafkeys[i].asaxt);
  }
//
//  exit(1);
  out("开查am");
  if (res_leafkeys_num) {
    //策略
    if (lookupi == 0) {
      int to_find_num = 1;
      get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
      res_p_num = res_leafkeys_num;
      for (int i=0;i<res_p_num;i++) {
        res_heap->Lock();
        float top = res_heap->top();
        int need = res_heap->need();
        res_heap->Unlock();
        if (need == 0 && top < res_p[i].first) break;
        //否则send
        char* tmpinfo = add_info;
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &top, sizeof(float));
        charcpy(tmpinfo, &to_find_num, sizeof(int));
        charcpy(tmpinfo, &res_p[i].second, sizeof(void*));
        size_t out_size;
        char* out;
        size_t to_find_size = to_find_size_leafkey + sizeof(void*);
        find_tskey(info, to_find_size, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        // 写堆
        res_heap->Lock();
        for (int j=0;j<out_size;j++) {
          if (!res_heap->push(ares_out[j])) break;
        }
        res_heap->Unlock();
        free(out);
      }
    }
    else if (lookupi == 1) {
      // 一部分 k个
      int to_find_num = aquery1.k;
      get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
      res_p_num = res_leafkeys_num;
      for (int i=0;i<res_p_num;i+=to_find_num) {
        int endi = min(i + to_find_num-1, res_p_num - 1);
        res_heap->Lock();;
        float top = res_heap->top();
        int need = res_heap->need();
        res_heap->Unlock();
        bool isbreak = false;
        if (need == 0) {
          if (top < res_p[endi].first) {
            int l=i,r=endi;
            while(l<r) {
              int mid = (l+r)/2;
              if(top < res_p[mid].first) r = mid;
              else l = mid + 1;
            }
            endi = l-1;
            isbreak = true;
          }
        }
        int findsize = endi - i + 1;
        if (!findsize) break;
        //否则send
        char* tmpinfo = add_info;
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &top, sizeof(float));
        charcpy(tmpinfo, &findsize, sizeof(int));
        for (int j=i;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
        size_t out_size;
        char* out;
        size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
        find_tskey(info, to_find_size, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        // 写堆
        res_heap->Lock();;
        for (int j=0;j<out_size;j++) {
          if (!res_heap->push(ares_out[j])) break;
        }
        res_heap->Unlock();
        free(out);
        if (isbreak) break;
      }
    }
    else if (lookupi == 2){
#if cha
      // 直接查一个叶
      res_heap->Lock();;
      float top = res_heap->top();
      int need = res_heap->need();
      res_heap->Unlock();
      int endi = res_leafkeys_num - 1;
      if (need == 0) {
        endi = get_dist_and_no_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p, top) - 1;
      }
      int findsize = endi + 1;
      if (findsize) {
        //否则send
        char* tmpinfo = add_info;
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &top, sizeof(float));
        charcpy(tmpinfo, &findsize, sizeof(int));
        if (need) for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_leafkeys[j].p, sizeof(void*));
        else for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
        size_t out_size;
        char* out;
        size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
        find_tskey(info, to_find_size, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        // 写堆
        res_heap->Lock();;
        for (int j=0;j<out_size;j++) {
          if (!res_heap->push(ares_out[j])) break;
        }
        res_heap->Unlock();
        free(out);
      }
#else
      res_heap->Lock();
      for(int i=0;i<res_leafkeys_num;i++) {
        res_heap->to_sort_dist_p.emplace_back(minidist_paa_to_saxt((ts_type*)aquery1.paa, res_leafkeys[i].asaxt.asaxt, Bit_cardinality), res_leafkeys[i].p);
      }
      res_heap->subUse();
      res_heap->isfinish();
      free(res_leafkeys);
      free(res_p);
      free(info);
      res_heap->Unlock();
      return;
#endif
    }
  }

#if cha==0
  res_heap->Lock();
  res_heap->subUse();
  res_heap->isfinish();
  free(res_leafkeys);
  free(res_p);
  free(info);
  res_heap->Unlock();
  return;
#endif

  bool isdel = false;
  bool isover = false;

  //必找的一个点找完后，
  res_heap->Lock();;
  res_heap->subUse();
  res_heap->isfinish();
  int need1 = res_heap->need();
  if (!need1) {
    isdel = res_heap->subOver();
    res_heap->Unlock();
    free(res_leafkeys);
    free(res_p);
    free(info);
    if (isdel) delete res_heap;
    return;
  }
  res_heap->Unlock();

  // 找非叶结点上的其他结点
  Finder.sort();
  for (auto & kk : Finder.has_cod){

    res_heap->Lock();;
    float top = res_heap->top();
    int need = res_heap->need();
    if (!need) {
      isdel = res_heap->subOver();
      res_heap->Unlock();
      free(res_leafkeys);
      free(res_p);
      free(info);
      if (isdel) delete res_heap;
      return;
    }
    res_heap->Unlock();
    Finder.find_One(res_leafkeys, res_leafkeys_num, (Leaf*)kk.second);

    if (res_leafkeys_num) {
      //策略
      if (lookupi == 0) {
        int to_find_num = 1;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i++) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &to_find_num, sizeof(int));
          charcpy(tmpinfo, &res_p[i].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*);
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 1) {
        // 一部分 k个
        int to_find_num = aquery1.k;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i+=to_find_num) {
          int endi = min(i + to_find_num-1, res_p_num - 1);
          int findsize = endi - i + 1;
          if (!findsize) break;
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=i;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 2){
        // 直接查一个叶
        int endi = res_leafkeys_num - 1;
        int findsize = endi + 1;
        if (findsize) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_leafkeys[j].p, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
    }
  }

  for (auto & kk : Finder.no_has_cod){

    res_heap->Lock();;
    float top = res_heap->top();
    int need = res_heap->need();
    if (!need) {
      isdel = res_heap->subOver();
      res_heap->Unlock();
      free(res_leafkeys);
      free(res_p);
      free(info);
      if (isdel) delete res_heap;
      return;
    }
    res_heap->Unlock();
    Finder.find_One(res_leafkeys, res_leafkeys_num, (Leaf*)kk);

    if (res_leafkeys_num) {
      //策略
      if (lookupi == 0) {
        int to_find_num = 1;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i++) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &to_find_num, sizeof(int));
          charcpy(tmpinfo, &res_p[i].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*);
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 1) {
        // 一部分 k个
        int to_find_num = aquery1.k;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i+=to_find_num) {
          int endi = min(i + to_find_num-1, res_p_num - 1);
          int findsize = endi - i + 1;
          if (!findsize) break;
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=i;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 2){
        // 直接查一个叶
        int endi = res_leafkeys_num - 1;
        int findsize = endi + 1;
        if (findsize) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_leafkeys[j].p, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
    }
  }

  res_heap->Lock();
  isdel = res_heap->subOver();
  res_heap->Unlock();

  free(res_leafkeys);
  free(res_p);
  free(info);
  if (isdel) delete res_heap;
}

void DBImpl::Get_st(const aquery& aquery1, query_heap* res_heap,
                    uint64_t st_number, Version* this_ver) {


  LeafKey* res_leafkeys = (LeafKey*)malloc(sizeof(LeafKey)*Leaf_rebuildnum);
  int res_leafkeys_num;
  auto res_p = (dist_p *)malloc(sizeof(dist_p)*Leaf_rebuildnum);
  int res_p_num;
  char* info = (char*)malloc(to_find_size_leafkey + sizeof(void*) * Leaf_rebuildnum);
  char* add_info = info;
  charcpy(add_info, &aquery1.rep, sizeof(aquery_rep));
  charcpy(add_info, &aquery1.k, sizeof(int));

  uint64_t filesize = this_ver->GetSize(st_number);
  Cache::Handle* file_handle = nullptr;
  Table* t = versions_->table_cache_->Get(st_number, filesize, file_handle);

#if istime == 2
  Table::ST_finder Finder(t, aquery1.asaxt, aquery1.rep.startTime, aquery1.rep.endTime, (ts_type*)aquery1.paa);
#else
  Table::ST_finder Finder(t, aquery1.asaxt, (ts_type*)aquery1.paa);
#endif
  Finder.root_Get();

  //必找的一个点
  Finder.find_One(res_leafkeys, res_leafkeys_num);

  for(int i=0;i<res_leafkeys_num;i++) {
    saxt_print(res_leafkeys[i].asaxt);
    out(res_leafkeys[i].p);
  }
//
//  exit(1);
  out("开查st");
  if (res_leafkeys_num) {
    //策略
    if (lookupi == 0) {
      int to_find_num = 1;
      get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
      res_p_num = res_leafkeys_num;
      for (int i=0;i<res_p_num;i++) {
        res_heap->Lock();;
        float top = res_heap->top();
        int need = res_heap->need();
        res_heap->Unlock();
        if (need == 0 && top < res_p[i].first) break;
        //否则send
        char* tmpinfo = add_info;
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &top, sizeof(float));
        charcpy(tmpinfo, &to_find_num, sizeof(int));
        charcpy(tmpinfo, &res_p[i].second, sizeof(void*));
        size_t out_size;
        char* out;
        size_t to_find_size = to_find_size_leafkey + sizeof(void*);
        find_tskey(info, to_find_size, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        // 写堆
        res_heap->Lock();;;
        for (int j=0;j<out_size;j++) {
          if (!res_heap->push(ares_out[j])) break;
        }
        res_heap->Unlock();
        free(out);
      }
    }
    else if (lookupi == 1) {
      // 一部分 k个
      int to_find_num = aquery1.k;
      get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
      res_p_num = res_leafkeys_num;
      for (int i=0;i<res_p_num;i+=to_find_num) {
        int endi = min(i + to_find_num-1, res_p_num - 1);
        res_heap->Lock();
        float top = res_heap->top();
        int need = res_heap->need();
        res_heap->Unlock();
        bool isbreak = false;
        if (need == 0) {
          if (top < res_p[endi].first) {
            int l=i,r=endi;
            while(l<r) {
              int mid = (l+r)/2;
              if(top < res_p[mid].first) r = mid;
              else l = mid + 1;
            }
            endi = l-1;
            isbreak = true;
          }
        }
        int findsize = endi - i + 1;
        if (!findsize) break;
        //否则send
        char* tmpinfo = add_info;
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &top, sizeof(float));
        charcpy(tmpinfo, &findsize, sizeof(int));
        for (int j=i;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
        size_t out_size;
        char* out;
        size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
        find_tskey(info, to_find_size, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        // 写堆
        res_heap->Lock();;;
        for (int j=0;j<out_size;j++) {
          if (!res_heap->push(ares_out[j])) break;
        }
        res_heap->Unlock();
        free(out);
        if (isbreak) break;
      }
    }
    else if (lookupi == 2){
#if cha
      // 直接查一个叶
      res_heap->Lock();
      float top = res_heap->top();
      int need = res_heap->need();
      res_heap->Unlock();
      int endi = res_leafkeys_num - 1;
      if (need == 0) {
        endi = get_dist_and_no_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p, top) - 1;
      }
      int findsize = endi + 1;
      if (findsize) {
        //否则send
        char* tmpinfo = add_info;
        charcpy(tmpinfo, &need, sizeof(int));
        charcpy(tmpinfo, &top, sizeof(float));
        charcpy(tmpinfo, &findsize, sizeof(int));
        if (need) for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_leafkeys[j].p, sizeof(void*));
        else for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
        size_t out_size;
        char* out;
        size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
        find_tskey(info, to_find_size, out, out_size, db_jvm);
        ares* ares_out = (ares*) out;
        // 写堆
        res_heap->Lock();
        for (int j=0;j<out_size;j++) {
//          out1("dist", ares_out[j].dist);
          if (!res_heap->push(ares_out[j])) break;
        }
        res_heap->Unlock();
        free(out);
      }
#else
      res_heap->Lock();
      for(int i=0;i<res_leafkeys_num;i++) {
        res_heap->to_sort_dist_p.emplace_back(minidist_paa_to_saxt((ts_type*)aquery1.paa, res_leafkeys[i].asaxt.asaxt, Bit_cardinality), res_leafkeys[i].p);
      }
      res_heap->subUse();
      res_heap->isfinish();
      versions_->table_cache_->cache_->Release(file_handle);
      free(res_leafkeys);
      free(res_p);
      free(info);
      res_heap->Unlock();
      return;
#endif
    }
  }

#if cha==0
  res_heap->Lock();
  res_heap->subUse();
  res_heap->isfinish();
  versions_->table_cache_->cache_->Release(file_handle);
  free(res_leafkeys);
  free(res_p);
  free(info);
  res_heap->Unlock();
  return;
#endif

  bool isdel = false;
  bool isover = false;

  //必找的一个点找完后，
  res_heap->Lock();
  res_heap->subUse();
  res_heap->isfinish();
  int need1 = res_heap->need();
  if (!need1) {
    isdel = res_heap->subOver();
    res_heap->Unlock();
    versions_->table_cache_->cache_->Release(file_handle);
    free(res_leafkeys);
    free(res_p);
    free(info);
    if (isdel) delete res_heap;
    return;
  }
  res_heap->Unlock();

  // 找非叶结点上的其他结点
  Finder.sort();
  for (auto & kk : Finder.has_cod){

    res_heap->Lock();
    float top = res_heap->top();
    int need = res_heap->need();
    if (!need) {
      isdel = res_heap->subOver();
      res_heap->Unlock();
      versions_->table_cache_->cache_->Release(file_handle);
      free(res_leafkeys);
      free(res_p);
      free(info);
      if (isdel) delete res_heap;
      return;
    }
    res_heap->Unlock();
    Finder.find_One(res_leafkeys, res_leafkeys_num, kk.second);

    if (res_leafkeys_num) {
      //策略
      if (lookupi == 0) {
        int to_find_num = 1;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i++) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &to_find_num, sizeof(int));
          charcpy(tmpinfo, &res_p[i].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*);
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 1) {
        // 一部分 k个
        int to_find_num = aquery1.k;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i+=to_find_num) {
          int endi = min(i + to_find_num-1, res_p_num - 1);
          int findsize = endi - i + 1;
          if (!findsize) break;
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=i;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 2){
        // 直接查一个叶
        int endi = res_leafkeys_num - 1;
        int findsize = endi + 1;
        if (findsize) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_leafkeys[j].p, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
    }
  }

  for (auto & kk : Finder.no_has_cod){

    res_heap->Lock();
    float top = res_heap->top();
    int need = res_heap->need();
    if (!need) {
      isdel = res_heap->subOver();
      res_heap->Unlock();
      versions_->table_cache_->cache_->Release(file_handle);
      free(res_leafkeys);
      free(res_p);
      free(info);
      if (isdel) delete res_heap;
      return;
    }
    res_heap->Unlock();
    Finder.find_One(res_leafkeys, res_leafkeys_num, kk);

    if (res_leafkeys_num) {
      //策略
      if (lookupi == 0) {
        int to_find_num = 1;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i++) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &to_find_num, sizeof(int));
          charcpy(tmpinfo, &res_p[i].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*);
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 1) {
        // 一部分 k个
        int to_find_num = aquery1.k;
        get_dist_and_sort((ts_type*)aquery1.paa, res_leafkeys, res_leafkeys_num, res_p);
        res_p_num = res_leafkeys_num;
        for (int i=0;i<res_p_num;i+=to_find_num) {
          int endi = min(i + to_find_num-1, res_p_num - 1);
          int findsize = endi - i + 1;
          if (!findsize) break;
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=i;j<=endi;j++) charcpy(tmpinfo, &res_p[j].second, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          //push
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
      else if (lookupi == 2){
        // 直接查一个叶
        int endi = res_leafkeys_num - 1;
        int findsize = endi + 1;
        if (findsize) {
          //否则send
          char* tmpinfo = add_info;
          charcpy(tmpinfo, &need, sizeof(int));
          charcpy(tmpinfo, &top, sizeof(float));
          charcpy(tmpinfo, &findsize, sizeof(int));
          for (int j=0;j<=endi;j++) charcpy(tmpinfo, &res_leafkeys[j].p, sizeof(void*));
          size_t out_size;
          char* out;
          size_t to_find_size = to_find_size_leafkey + sizeof(void*) * findsize;
          find_tskey(info, to_find_size, out, out_size, db_jvm);
          ares* ares_out = (ares*) out;
          // 写堆
          res_heap->Lock();;;
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          for (int j=0;j<out_size;j++) {
            if (!res_heap->push(ares_out[j])) break;
          }
          res_heap->isfinish();
          need = res_heap->need();
          if (!need) {
            isdel = res_heap->subOver();
            res_heap->Unlock();
            versions_->table_cache_->cache_->Release(file_handle);
            free(res_leafkeys);
            free(res_p);
            free(info);
            free(out);
            if (isdel) delete res_heap;
            return;
          }
          top = res_heap->top();
          res_heap->Unlock();
          free(out);
        }
      }
    }
  }

  res_heap->Lock();
  isdel = res_heap->subOver();
  res_heap->Unlock();

  versions_->table_cache_->cache_->Release(file_handle);
  free(res_leafkeys);
  free(res_p);
  free(info);
  if (isdel) delete res_heap;
}

Status DBImpl::Get_exact(const aquery& aquery1, int am_version_id,
                         int st_version_id, jniVector<uint64_t>& st_number,
                         jniVector<ares>& appro_res, jniVector<ares_exact>& results,
                         int appro_am_id, jniVector<uint64_t> &appro_st_number, jniInfo jniInfo_) {

//  cout<<"精确"<<endl;
  out("开始查询精确=========================");
  out("k:"+to_string(aquery1.k));
#if istime
  out("starttime:"+to_string(aquery1.rep.startTime));
  out("endtime:"+to_string(aquery1.rep.endTime));
#endif
  out1("am_version_id", am_version_id);
  out1("st_version_id:", st_version_id);
  out("开始查询精确000000000000000000000000");


  mutex_Mem.Lock();
  mem_version* this_mem_version = memSet->OldVersion(am_version_id);
  this_mem_version->Ref();
  mutex_Mem.Unlock();
  //看所有表
  vector<MemTable*> this_mems = this_mem_version->Get();
  vector<int> to_find_mems;
  for(int i=0;i<this_mems.size();i++) {
#if istime
    MemTable* item = this_mems[i];
    if (!(item->startTime > aquery1.rep.endTime ||
        item->endTime < aquery1.rep.startTime)) {
      to_find_mems.push_back(i);
    }
#else
    to_find_mems.push_back(i);
#endif
  }

  //创建堆
  query_heap_exact* res_heap = new query_heap_exact(aquery1.k, st_number.size()+to_find_mems.size());
//  cout<<"size："<<st_number.size()+to_find_mems.size()<<endl;
  res_heap->vv1 = this_mem_version;
  res_heap->v1_mutex = &mutex_Mem;
  if (!st_number.empty()) {
    mutex_.Lock();
    version_map[st_version_id]->Ref();
    res_heap->vv2 = version_map[st_version_id];
    res_heap->v2_mutex = &mutex_;
    mutex_.Unlock();
  }
  res_heap->init(appro_res);
  out1("topdist", res_heap->top());

  res_heap->Lock();


  for(auto item: to_find_mems) {
    pool_get->enqueue(&DBImpl::BGWork_Get_am_exact, this, aquery1, res_heap, this_mems[item], appro_am_id == item);
//    std::thread athread(&DBImpl::BGWork_Get_am_exact, this, aquery1, res_heap, this_mems[item], appro_am_id == item);
//    athread.detach();
  }

  for (int o=0;o<st_number.size();o++) {
    int j = st_number[o];
    bool isappro = false;
    for(int oo=0;oo<appro_st_number.size();oo++) {
      int appro_st_j = appro_st_number[oo];
      if (appro_st_j==j) isappro = true;
    }
    pool_get->enqueue(&DBImpl::BGWork_Get_st_exact, this, aquery1, res_heap, j, res_heap->vv2, isappro);
//    std::thread sthread(&DBImpl::BGWork_Get_st_exact, this, aquery1, res_heap, j, res_heap->vv2, isappro);
//    sthread.detach();
  }

  //等待结果
  res_heap->wait();
  // 按dist排序，分成20份依次查询
  res_heap->sort_dist_p();

  int div = Get_div;
  div = max(div, (int)(res_heap->to_sort_dist_p.size()-1)/info_p_max_size+1);
  int num_per = (res_heap->to_sort_dist_p.size()-1)/div+1;
//  cout<<"size"<<res_heap->to_sort_dist_p.size()<<endl;
//  char* info = (char*)malloc(to_find_size_leafkey + sizeof(void*) * num_per);
  char* info = jniInfo_.info_p;
  char* add_info = info;
  charcpy(add_info, &aquery1.rep, sizeof(aquery_rep));
#if isap
  charcpy(add_info, &res_heap, sizeof(void*));
#endif
  charcpy(add_info, &aquery1.k, sizeof(int));
  bool isover = false;
  for(int i=0;i<div;i++) {
    int num_one = min((i+1)*num_per, (int)res_heap->to_sort_dist_p.size()) - i*num_per;
    char* tmpinfo = add_info;
    int need = res_heap->need();
    float topdist = res_heap->top();
    charcpy(tmpinfo, &need, sizeof(int));
    charcpy(tmpinfo, &topdist, sizeof(float));
    char* numinfo = tmpinfo;
    tmpinfo += sizeof(int);
    int real_num = 0;
    for(int j=i*num_per;j<i*num_per+num_one;j++){
//      cout<<"j:"<<j<<" "<<res_heap->to_sort_dist_p[j].first<<" "<<res_heap->to_sort_dist_p[j].second<<endl;
      if (res_heap->to_sort_dist_p[j].first <= topdist)
      charcpy(tmpinfo, &res_heap->to_sort_dist_p[j].second, sizeof(void*)), real_num++;
      else {
        isover = true;
        break;
      }
    }
    charcpy(numinfo, &real_num, sizeof(int));
#if isap
//    find_tskey_exact_ap(info, to_find_size_leafkey + sizeof(void*) * real_num, db_jvm);
    find_tskey_exact_ap_buffer(jniInfo_, db_jvm);
#else
    char* out;
    size_t out_size;
    find_tskey_exact(info, to_find_size_leafkey + sizeof(void*) * real_num, out, out_size, db_jvm);
    ares_exact* ares_out = (ares_exact*) out;
    //push
    for (int j=0;j<out_size;j++) {
      if (!res_heap->push(ares_out[j])) break;
    }
    free(out);
#endif
    if(isover) break;
  }


  res_heap->get(results);
  bool isdel = res_heap->subOver();
#if iscount_saxt_for_exact
  res_heap->mutex_saxt_num.lock();
  out1("计算下界距离saxt的数量:", res_heap->saxt_num_exact);
  res_heap->mutex_saxt_num.unlock();
#endif
  res_heap->Unlock();
  if (isdel) delete res_heap;


//  free(info);
  return Status();
}

void DBImpl::BGWork_Get_am_exact(void* db, const aquery& aquery1,
                                 query_heap_exact* res_heap, MemTable* to_find_mem, bool isappro) {
  reinterpret_cast<DBImpl*>(db)->Get_am_exact(aquery1, res_heap, to_find_mem, isappro);
}

void DBImpl::BGWork_Get_st_exact(void* db, const aquery& aquery1,
                                 query_heap_exact* res_heap, uint64_t st_number,
                           Version* this_ver, bool isappro) {
  reinterpret_cast<DBImpl*>(db)->Get_st_exact(aquery1, res_heap, st_number, this_ver, isappro);
}

void DBImpl::Get_am_exact(const aquery& aquery1, query_heap_exact* res_heap,
                    MemTable* to_find_mem, bool isappro) {

  if (isappro){

    ZsbTree_finder_exact_appro Finder(aquery1, res_heap, to_find_mem->GetRoot(), db_jvm);

    Finder.start();

#if iscount_saxt_for_exact
    mutex_saxt_num.Lock();
  saxt_num_exact += Finder.saxt_num;
  mutex_saxt_num.Unlock();
#endif

  } else {

    ZsbTree_finder_exact Finder(aquery1, res_heap, to_find_mem->GetRoot(), db_jvm);

    Finder.start();

#if iscount_saxt_for_exact
    mutex_saxt_num.Lock();
  saxt_num_exact += Finder.saxt_num;
  mutex_saxt_num.Unlock();
#endif

  }

  res_heap->Lock();
  res_heap->subOver();
  res_heap->Unlock();

//  if (isdel) delete res_heap;

}
static int x1 = 0;
void DBImpl::Get_st_exact(const aquery& aquery1, query_heap_exact* res_heap,
                    uint64_t st_number, Version* this_ver, bool isappro) {

  out1("get_st_exact", st_number);
//  cout<<"get_st_exact:"+st_number<<endl;
//  cout<<"jia"<<++x1<<endl;
  uint64_t filesize = this_ver->GetSize(st_number);
  Cache::Handle* file_handle = nullptr;
  Table* t = versions_->table_cache_->Get(st_number, filesize, file_handle);
  if (isappro) {

    Table::ST_finder_exact_appro Finder(t, (aquery*)&aquery1, res_heap, db_jvm, filesize >= get_exact_multiThread_file_size, pool_get);

    Finder.start();

  } else {

    Table::ST_finder_exact Finder(t, (aquery*)&aquery1, res_heap, db_jvm, filesize >= get_exact_multiThread_file_size, pool_get);

    Finder.start();



  }


  res_heap->Lock();
  res_heap->subOver();
  res_heap->Unlock();

//  cout<<"get_st_exact_zhong:"+st_number<<endl;
  versions_->table_cache_->cache_->Release(file_handle);
//  if (isdel) delete res_heap;
//  cout<<"get_st_exact_wan:"+st_number<<endl;
//  cout<<"jian"<<--x1<<endl;
  out1("get_st_exact_finish", st_number);
}




Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}



// Convenience methods
//选表
Status DBImpl::Put(const WriteOptions& o, LeafTimeKey& key) {
//  WriteBatch batch;
//  batch.Put(key);
  // 找到对应的drange
  // 二分
  int l = 0;
//  int r = mems.size()-1;
//
//  while (l < r) {
//    int mid = (l + r + 1) / 2;
//    if (bounds[mid] <= key.leafKey.asaxt) l = mid;
//    else r = mid - 1;
//  }
//  port::Mutex* mutex_i = write_mutex.data() + l;
//  mutex_i->Lock();

  //读写锁
  {
    // 这个是大平衡需要
    boost::shared_lock<boost::shared_mutex> lock1(range_mutex);

    //顺序
    int i = 1;
    for(;i<mems.size();i++){
      if (bounds[i] > key.leafKey.asaxt)
        break;
    }
    l = i - 1;
    //必须要插入这个表了
    write_mutex[l].Lock();
  }


  memNum_period[l]++;
  Write(o, key, l);

  return Status();
}


Status DBImpl::Init(LeafTimeKey* leafKeys, int leafKeysNum) {

  Status status;
  LeafKey* leafkeys_rep = (LeafKey*)malloc(leafKeysNum*sizeof(LeafKey));

#if istime > 0
  ts_time* times_rep = (ts_time*)malloc(leafKeysNum*sizeof(ts_time));
#endif
  for(int i=0;i<leafKeysNum;i++){
    leafkeys_rep[i] = leafKeys[i].leafKey;
#if istime > 0
    times_rep[i] = leafKeys[i].keytime;
#endif
  }
  vector<NonLeafKey> nonLeafKeys;
  newVector<LeafKey> leafKeys_(leafkeys_rep, 0, leafKeysNum);
  leaf_method::buildtree(leafKeys_, nonLeafKeys, Leaf_maxnum, Leaf_minnum);
//  long long sum = 0;
//  for(int i=0;i<nonLeafKeys.size();i++){
//    sum += nonLeafKeys[i].co_d;
//  }
//  cout<< (double)sum / nonLeafKeys.size() << " "<<nonLeafKeys.size();
//  exit(1);
  free(leafkeys_rep);

  int drangesNum = leafKeysNum / Table_maxnum;
  //这个range的总数。
  int averageNum = leafKeysNum / drangesNum  - Leaf_minnum;
  //创建一个边界版本


  //贪心 返回起始位置
  int timeid = 0;
  saxt_only last_r_saxt;
  write_mutex.reserve(drangesNum);
  write_signal_.reserve(drangesNum);
  writers_vec[0].reserve(drangesNum);
  writers_vec[1].reserve(drangesNum);
  mems.reserve(drangesNum);
  memNum.reserve(drangesNum);
  memNum_period.reserve(drangesNum);
  bounds.reserve(drangesNum);
  for (int i = 0, j = 0; i < drangesNum; ++i) {
    int start = j;
    write_mutex.emplace_back();
    write_signal_.emplace_back(&write_mutex.back());
    writers_vec[0].emplace_back();
    writers_vec[1].emplace_back();
    writers_vec[0].back().reserve(8192);
    writers_vec[1].back().reserve(8192);
    towrite[i] = 0;
    writers_is[i] = false;
    imms_isdoing[i] = false;
    int sum = 0;
    if (i<drangesNum-1){
      while (sum < averageNum && j < nonLeafKeys.size()){
        sum += nonLeafKeys[j++].num;
      }
    } else {
      while (j < nonLeafKeys.size()){
        sum += nonLeafKeys[j++].num;
      }
    }
    memNum.push_back(sum);
    memNum_period.push_back(0);
    int newtimeid = timeid + sum;
    ts_time mintime = 0x3f3f3f3f3f3f3f3f;
    ts_time maxtime = 0;
#if istime > 0
    for(int k=timeid;k<newtimeid;k++){
      mintime = min(mintime, times_rep[k]);
      maxtime = max(maxtime, times_rep[k]);
    }
#endif
    timeid = newtimeid;
    out("sum");
    out(sum);
    newVector<NonLeafKey> drangeNonLeafKeys(nonLeafKeys, start, j);
    MemTable* newMem = new MemTable(mintime, maxtime, &memsTodel);
    newMem->table_.BuildTree(drangeNonLeafKeys);
    mems.push_back(newMem);
    //构建边界
    if(!i) {
      //第一个,直接放入全0
      saxt_only newsaxt;
      memset(newsaxt.asaxt, 0, sizeof(saxt_only));
      bounds.push_back(newsaxt);
    } else {
      //返回第一个不同的中间的数，后面赋值全1和全0
      saxt_only l_saxt = newMem->Getlsaxt();
      cod co_d = get_co_d_from_saxt(last_r_saxt, l_saxt);
      if (co_d == 8){
        bounds.push_back(l_saxt);
      } else {
        int mid = (l_saxt.asaxt[Bit_cardinality-1-co_d] + last_r_saxt.asaxt[Bit_cardinality-1-co_d]) / 2;
        if (mid == last_r_saxt.asaxt[Bit_cardinality-1-co_d]) mid++;
        saxt_only newsaxt = l_saxt;
        newsaxt.asaxt[Bit_cardinality-1-co_d] = mid;
        memset(newsaxt.asaxt, 0, (Bit_cardinality - co_d - 1)*sizeof(saxt_type));
        bounds.push_back(newsaxt);
      }

    }

    last_r_saxt = newMem->Getrsaxt();
  }
#if istime > 0
  free(times_rep);
#endif
  mems_boundary* newb = new mems_boundary(bounds);
  mem_version* newMemVersion = new mem_version(mems, newb);
  memSet->newversion(newMemVersion);
//  for(int i=0;i<mems.size();i++) {
//    saxt_print(mems[i]->Getlsaxt());
//    saxt_print(mems[i]->Getrsaxt());
//  }
//  out("bounds");
//  for(int i=0;i<mems.size();i++) {
//    saxt_print(bounds[i].asaxt);
//  }

  if (init_st) {
    out("初始化st");
    for (int memId = 0; memId < mems.size(); memId++) {
//      sleep(1);
      while (true) {
        MutexLock l(&mutex_);
        if (imms_isdoing[memId]) {
//        out("waitim");
          // We have filled up the current memtable, but the previous
          // one is still being compacted, so we wait.
          out("当前表的范围有im在处理，等待===============");
          Log(options_.info_log, "Current memtable full; waiting...\n");
          background_work_finished_signal_.Wait();
//        out("waitoverim");
        } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
          // There are too many level-0 files.
          out("0层st过多等待");
          Log(options_.info_log, "Too many L0 files; waiting...\n");
          MaybeScheduleCompaction();
          background_work_finished_signal_.Wait();
        } else {
          // Attempt to switch to a new memtable and trigger compaction of old
          //重置了log
//        assert(versions_->PrevLogNumber() == 0);
//        uint64_t new_log_number = versions_->NewFileNumber();
//        WritableFile* lfile = nullptr;
//        s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
//        if (!s.ok()) {
//          // Avoid chewing through file number space in a tight loop.
//          versions_->ReuseFileNumber(new_log_number);
//          break;
//        }
//        delete log_;
//        delete logfile_;
//        logfile_ = lfile;
//        logfile_number_ = new_log_number;
//        log_ = new log::Writer(lfile);
          //转为im

          MemTable* oldmem = mems[memId];
          // 复制mem的树结构
          MemTable* newmem = new MemTable(oldmem);
          newmem->Ref();
          // 修改版本
          mems[memId] = newmem;
//          for(auto item:mems) assert(item->refs_>0);
          imms.emplace_back(oldmem, memId);
          imms_isdoing[memId] = true;
          has_imm_.store(true, std::memory_order_release);
          memNum[memId] = 0;
          MaybeScheduleCompaction();
          break;
        }
      }
    }
  }


  out("初始化完毕");
  return status;
}



Status DBImpl::RebalanceDranges() {
  //读写锁
  boost::unique_lock<boost::shared_mutex> lock1(range_mutex);

  int m = memNum_period.size();
  bool *st = (bool*)malloc(sizeof(bool)*m);
  //获得所有表的锁
  while (m){
    for(int i=0;i<memNum_period.size();i++){
      if (!st[i]){
        unique_lock<mutex> g(write_mutex[i].mu_, try_to_lock);
        if (g.owns_lock() && writers_vec[0][i].empty() && writers_vec[1][i].empty()){
          //获得写锁，发现队列为空
          //就一直锁住
          g.release();
          m--;
          st[i] = true;
        }
      }
    }
  }

  free(st);

  vector<pair<int, int>> res = get_drange_rebalance(memNum_period);



  for (auto item: res) {
    vector<int> todo_dranges;
    for(int i=item.first;i<=item.second;i++) todo_dranges.push_back(i);
    RebalanceDranges(todo_dranges);
  }

  for (int i=0;i<write_mutex.size();i++){
    memNum_period[i] = 0;
    write_mutex[i].Unlock();
  }
  return Status();
}


Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
//  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options,LeafTimeKey& key, int memId) {

  port::Mutex* mutex_i = write_mutex.data() + memId;
  mutex_i->AssertHeld();
//  out("Write"+to_string(memId));

  if (writers_is[memId]) {
//    out("进数组"+to_string(memId));
    //先删除一下没用的表
    //有空的线程
    vector<LeafTimeKey>* w = &writers_vec[towrite[memId]][memId];
    w->push_back(key);
    if (w->size()>=8100) {
      int id = w->size();
//      out(to_string(memId)+"进入"+to_string(id));
      write_signal_[memId].Wait();
//      out("解锁"+to_string(memId)+"进入"+to_string(id));
      mutex_i->Unlock();
      return Status();
    }
    mutex_i->Unlock();
    return Status();
  }
//  out("不进数组"+to_string(memId));
  vector<LeafTimeKey>* w = &writers_vec[towrite[memId]][memId];
  writers_is[memId] = true;
  w->push_back(key);
  while (!w->empty()) {
    assert(mems[memId]->refs_>0);
    towrite[memId] = 1 - towrite[memId];
    Status status = MakeRoomForWrite(false, memId);
    assert(status.ok());
    if (status.ok()) {  // nullptr batch is for compactions
      int nowNum = memNum[memId];
      memNum[memId] += w->size();
      {
        mutex_i->Unlock();
        // fileOffset拿到后就可以构造leafkey，把前8位用来做位于一个record的位次，最多256个
        if (status.ok()) {
          //里面会有drange内的平衡
          //        out("todoocharu");
          status = WriteBatchInternal::InsertInto(*w, mems, &mutex_Mem,
                                                  nowNum, memId, memSet);
          //        out("finishcharu");
          // 一个batch插入完后，在更新。
        }
        mutex_i->Lock();
      }
    }
//    out(to_string(memId)+"解锁");
    w->clear();
    assert(&writers_vec[towrite[memId]][memId] != w);
    w = &writers_vec[towrite[memId]][memId];
    if (w->size()>=8100) {
//      out("要解锁1"+to_string(memId));
      write_signal_[memId].SignalAll();
      writers_is[memId] = false;
//      out("要解锁2"+to_string(memId));
      mutex_i->Unlock();
//      out("出去解锁223"+to_string(memId));
      return Status();
    }
  }

  writers_is[memId] = false;
  mutex_i->Unlock();
  return Status();

}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer, int memId) {
//  write_mutex[memId].AssertHeld();
////  assert(!writers_.empty());
//  Writer* first = writers_vec[memId].front();
//  WriteBatch* result = first->batch;
//  assert(result != nullptr);
//
//  size_t size = WriteBatchInternal::ByteSize(first->batch);
//
//  // Allow the group to grow up to a maximum size, but if the
//  // original write is small, limit the growth so we do not slow
//  // down the small write too much.
//  size_t max_size = 35850;
////  size_t max_size = 1 << 20;
////  if (size <= (128 << 10)) {
////    max_size = size + (128 << 10);
////  }
//
//  *last_writer = first;
//  std::deque<Writer*>::iterator iter = writers_vec[memId].begin();
//  ++iter;  // Advance past "first"
//  for (; iter != writers_vec[memId].end(); ++iter) {
//    Writer* w = *iter;
//    if (w->sync && !first->sync) {
//      // Do not include a sync write into a batch handled by a non-sync write.
//      break;
//    }
//
//    if (w->batch != nullptr) {
//      size += WriteBatchInternal::ByteSize(w->batch);
//      if (size > max_size) {
//        // Do not make batch too big
//        break;
//      }
//
//      // Append to *result
//      if (result == first->batch) {
//        // Switch to temporary batch instead of disturbing caller's batch
//        result = tmp_batchs[memId];
//        assert(WriteBatchInternal::Count(result) == 0);
//        WriteBatchInternal::Append(result, first->batch);
//      }
//      WriteBatchInternal::Append(result, w->batch);
//    }
//    *last_writer = w;
//  }
//  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force, int memId) {

  write_mutex[memId].AssertHeld();
//  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      write_mutex[memId].Unlock();
      out("st0太多，等1ms======================");
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      write_mutex[memId].Lock();
    } else if (!force &&
               (memNum[memId] <= Table_maxnum - Leaf_minnum)) {
//      if (memNum[memId]>99995){
//        out("memId");
//        out(memId);
//        out(memNum[memId]);
//      }
      // There is room in current memtable
//      out("todoput");
      break;
    } else {

      delete_mems(memsTodel.get());
//      out("makeroom_to_st");
      //上锁，因为im是共享的,目前只有一个，只要表满了，就进入一个共享了
      MutexLock l(&mutex_);
      if (imms_isdoing[memId]) {
//        out("waitim");
        // We have filled up the current memtable, but the previous
        // one is still being compacted, so we wait.
        out("当前表的范围有im在处理，等待===============");
        Log(options_.info_log, "Current memtable full; waiting...\n");
        background_work_finished_signal_.Wait();
//        out("waitoverim");
      } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
        // There are too many level-0 files.
        out("0层st过多等待");
        Log(options_.info_log, "Too many L0 files; waiting...\n");
        background_work_finished_signal_.Wait();
      } else {
        // Attempt to switch to a new memtable and trigger compaction of old
        //重置了log
//        assert(versions_->PrevLogNumber() == 0);
//        uint64_t new_log_number = versions_->NewFileNumber();
//        WritableFile* lfile = nullptr;
//        s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
//        if (!s.ok()) {
//          // Avoid chewing through file number space in a tight loop.
//          versions_->ReuseFileNumber(new_log_number);
//          break;
//        }
//        delete log_;
//        delete logfile_;
//        logfile_ = lfile;
//        logfile_number_ = new_log_number;
//        log_ = new log::Writer(lfile);
        //转为im

        MemTable* oldmem = mems[memId];
        // 复制mem的树结构
        MemTable* newmem = new MemTable(oldmem);
        newmem->Ref();
        // 修改版本
        mems[memId] = newmem;

        imms.emplace_back(oldmem, memId);
        imms_isdoing[memId] = true;
        has_imm_.store(true, std::memory_order_release);
        memNum[memId] = 0;
        force = false;  // Do not force another compaction if have room
        MaybeScheduleCompaction();
      }
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
//  value->clear();
//
//  MutexLock l(&mutex_);
//  Slice in = property;
//  Slice prefix("leveldb.");
//  if (!in.starts_with(prefix)) return false;
//  in.remove_prefix(prefix.size());
//
//  if (in.starts_with("num-files-at-level")) {
//    in.remove_prefix(strlen("num-files-at-level"));
//    uint64_t level;
//    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
//    if (!ok || level >= config::kNumLevels) {
//      return false;
//    } else {
//      char buf[100];
//      std::snprintf(buf, sizeof(buf), "%d",
//                    versions_->NumLevelFiles(static_cast<int>(level)));
//      *value = buf;
//      return true;
//    }
//  } else if (in == "stats") {
//    char buf[200];
//    std::snprintf(buf, sizeof(buf),
//                  "                               Compactions\n"
//                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
//                  "--------------------------------------------------\n");
//    value->append(buf);
//    for (int level = 0; level < config::kNumLevels; level++) {
//      int files = versions_->NumLevelFiles(level);
//      if (stats_[level].micros > 0 || files > 0) {
//        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
//                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
//                      stats_[level].micros / 1e6,
//                      stats_[level].bytes_read / 1048576.0,
//                      stats_[level].bytes_written / 1048576.0);
//        value->append(buf);
//      }
//    }
//    return true;
//  } else if (in == "sstables") {
//    *value = versions_->current()->DebugString();
//    return true;
//  } else if (in == "approximate-memory-usage") {
//    size_t total_usage = options_.block_cache->TotalCharge();
//    if (mem_) {
//      total_usage += mem_->ApproximateMemoryUsage();
//    }
//    if (imm_) {
//      total_usage += imm_->ApproximateMemoryUsage();
//    }
//    char buf[50];
//    std::snprintf(buf, sizeof(buf), "%llu",
//                  static_cast<unsigned long long>(total_usage));
//    value->append(buf);
//    return true;
//  }
//
//  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

void DBImpl::RebalanceDranges(vector<int>& table_rebalanced) {

  //todo 把im压缩了
  for (int memId = 0; memId < table_rebalanced.size(); memId++) {
//      sleep(1);
    while (true) {
      MutexLock l(&mutex_);
      if (imms_isdoing[memId]) {
//        out("waitim");
        // We have filled up the current memtable, but the previous
        // one is still being compacted, so we wait.
        out("当前表的范围有im在处理，等待===============");
        Log(options_.info_log, "Current memtable full; waiting...\n");
        background_work_finished_signal_.Wait();
//        out("waitoverim");
      } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
        // There are too many level-0 files.
        out("0层st过多等待");
        Log(options_.info_log, "Too many L0 files; waiting...\n");
        MaybeScheduleCompaction();
        background_work_finished_signal_.Wait();
      } else {
        // Attempt to switch to a new memtable and trigger compaction of old
        //重置了log
//        assert(versions_->PrevLogNumber() == 0);
//        uint64_t new_log_number = versions_->NewFileNumber();
//        WritableFile* lfile = nullptr;
//        s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
//        if (!s.ok()) {
//          // Avoid chewing through file number space in a tight loop.
//          versions_->ReuseFileNumber(new_log_number);
//          break;
//        }
//        delete log_;
//        delete logfile_;
//        logfile_ = lfile;
//        logfile_number_ = new_log_number;
//        log_ = new log::Writer(lfile);
        //转为im

        MemTable* oldmem = mems[memId];
        // 复制mem的树结构
        MemTable* newmem = new MemTable(oldmem);
        newmem->Ref();
        // 修改版本
        mems[memId] = newmem;
//          for(auto item:mems) assert(item->refs_>0);
        imms.emplace_back(oldmem, memId);
        imms_isdoing[memId] = true;
        has_imm_.store(true, std::memory_order_release);
        memNum[memId] = 0;
        MaybeScheduleCompaction();
        break;
      }
    }
  }



  vector<int> memsLeafNum;
  int leafNum_all = 0;

  for(auto item: table_rebalanced) {
    memsLeafNum.push_back(mems[item]->GetleafNum());
    leafNum_all += memsLeafNum.back();
  }
  vector<NonLeafKey> nonLeafKeys;
  nonLeafKeys.reserve(leafNum_all);
  vector<int> leafNum_period;
  leafNum_period.reserve(leafNum_all);

  int averageNum = 0;
  for(int k=0;k<table_rebalanced.size();k++){
    int i = table_rebalanced[k];
    averageNum += memNum_period[i];
    mems[i]->LoadNonLeafKeys(nonLeafKeys);
    int thisleafNum_period = memNum_period[i] / memsLeafNum[k];
    for(int j=0;j<memsLeafNum[k];j++) {
      leafNum_period.push_back(thisleafNum_period);
    }
  }
  averageNum = (averageNum - 1) / table_rebalanced.size() + 1;
  for (int k = 0, j = 0; k < table_rebalanced.size(); ++k) {
    int i = table_rebalanced[k];
    int start = j;
    int sum_period = 0;
    int sum = 0;
    while (sum_period < averageNum && j < nonLeafKeys.size()){
      sum_period += leafNum_period[j];
      sum += nonLeafKeys[j++].num;
    }
    //真实数量
    newVector<NonLeafKey> drangeNonLeafKeys(nonLeafKeys, start, j);
    MemTable* oldmem = mems[i];
    MemTable *newmem = new MemTable(BuildTree_new(drangeNonLeafKeys), &memsTodel);
    mutex_Mem.Lock();
    memSet->newversion_small(newmem, oldmem, i);
    mems[i] = newmem;
    mutex_Mem.Unlock();
  }

}

void DBImpl::UnRef_am(int unref_version_id) {
  out("unrefam"+to_string(unref_version_id));
  mutex_Mem.Lock();
  memSet->Unref(unref_version_id);
  mutex_Mem.Unlock();
  out("unrefam完成");
}

void DBImpl::UnRef_st(int unref_version_id) {
  out("unrefst"+to_string(unref_version_id));
  mutex_.AssertHeld();
  version_map[unref_version_id]->Unref();
  version_map.erase(unref_version_id);
  out("unrefst完成");
}


// Default implementations of convenience methods that subclasses of DB
// can call if they wish
//Status DB::Put(const WriteOptions& opt, const putKey &key) {
//  WriteBatch batch;
//  batch.Put(key);
//  return Write(opt, &batch, 0);
//}


Status DB::Delete(const WriteOptions& opt, const Slice& key) {
//  WriteBatch batch;
//  batch.Delete(key);
//  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, const void* db_jvm, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname, db_jvm);
  out("声明");
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  out("解决日志");
  if (s.ok() && !impl->mems.size()) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
//      impl->mem_ = new MemTable(impl->internal_comparator_);
//      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
//    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
