// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"

#include "db/write_batch_internal.h"
#include <util/mutexlock.h>



#include "util/coding.h"
#include "iostream"


namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

Status WriteBatch::Iterate(Handler* handler, int memNum) const {
//  Slice input(rep_);
//  if (input.size() < kHeader) {
//    return Status::Corruption("malformed WriteBatch (too small)");
//  }
//
//  input.remove_prefix(kHeader);
//  //从0开始
//  int found = -1;
//  while (!input.empty()) {
//    found++;
//    if (input.size() >= sizeof(LeafTimeKey)) {
//      //先改变memtable的时间
//      LeafTimeKey* leafTimeKey = (LeafTimeKey*)input.data();
//#if istime > 0
//      handler->SetTime(leafTimeKey->keytime);
//#endif
//      //我们添加进table 如果满了，重组
//      if (!handler->Put(leafTimeKey->leafKey)) {
//        out("重组");
//        out(memNum + found + 1);
//        int Nt = memNum + found + 1;
//        int nt = max(Nt * Leaf_maxnum / Table_maxnum, Leaf_maxnum_rebalance);
//
//        handler->Rebalance(nt, nt/2, Nt);
//      }
//      input.remove_prefix(sizeof(LeafTimeKey));
//    } else {
//      return Status::Corruption("bad WriteBatch Put");
//    }
//  }
//  found++;
//  if (found != WriteBatchInternal::Count(this)) {
//    return Status::Corruption("WriteBatch has wrong count");
//  } else {
//    return Status::OK();
//  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const LeafTimeKey& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.append((char*)&key, sizeof(LeafTimeKey));
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
static int xxx = 0;
class MemTableInserter : public WriteBatch::Handler {
 public:
//  SequenceNumber sequence_;
  MemTable* mem_;
  vector<MemTable*>& mems;
  port::Mutex* mutex_;
  mem_version_set* versionSet;
  int memId;
  MemTableInserter(vector<MemTable*>& mems, port::Mutex* mutex, int memId, mem_version_set* versionSet)
      : mem_(mems[memId]), mems(mems), mutex_(mutex), memId(memId), versionSet(versionSet){}

  bool Put(LeafKey& key) override {
//    unsigned char tmp[8] = {23 ,115 ,97 ,198, 124 ,242, 132 ,248 };
//    if(key.asaxt == *(saxt_only*)tmp) {
//      xxx++;
//      out("adsdd");
//      printf("%ld\n", (long)key.p);
//      if(xxx == 2 && 7772777 == (long)key.p) exit(10);
//      if(xxx == 5) exit(11);
//    }
    return mem_->Add(key);
  }

  void SetTime(ts_time newtime) override {
//    if(newtime>100000000000)
//      exit(1);
    if (mem_->endTime < newtime) mem_->endTime = newtime;
    if (mem_->startTime > newtime) mem_->startTime = newtime;
  }


  void Rebalance(int tmp_leaf_maxnum, int tmp_leaf_minnum, int Nt) override {
    out("叶子最大:"+to_string(tmp_leaf_maxnum));
    out("重组数量"+to_string(Nt));
    MemTable* oldmem = mem_;
    MemTable *newmem = oldmem->Rebalance(tmp_leaf_maxnum, tmp_leaf_minnum, Nt);
    out("重组==");
    mutex_->Lock();
    versionSet->newversion_small(newmem, oldmem, memId);
    mems[memId] = newmem;
    mutex_->Unlock();
    mem_ = mems[memId];
  }


  void Delete(const Slice& key) override {
//    mem_->Add(sequence_, kTypeDeletion, key, Slice());
//    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(vector<LeafTimeKey>& b, vector<MemTable*>& memtable, port::Mutex* mutex, int memNum, int memId, mem_version_set* versionSet) {
//  out("插入"+to_string(memId));
  MemTableInserter inserter(memtable, mutex, memId, versionSet);
  int found = 0;

  for(auto item: b) {
    found++;
#if istime > 0
    inserter.SetTime(item.keytime);
#endif
    //我们添加进table 如果满了，重组
    if (!inserter.Put(item.leafKey)) {
      out("重组"+to_string(memId));
//      out(memNum + found);
      int Nt = memNum + found;
      int nt = max(Nt * Leaf_maxnum / memtable_size, Leaf_maxnum_rebalance);

      inserter.Rebalance(nt, nt/2, Nt);
      out(to_string(memId)+"重组成功");
    }
  }
  return Status();
}

//Status WriteBatchInternal::InsertInto(LeafTimeKey& b, MemTable*& memtable, port::Mutex* mutex, int memNum, int memId, mem_version_set* versionSet) {
//  MemTableInserter inserter(memtable, mutex, memId, versionSet);
//  int found = 1;
//  inserter.SetTime(b.keytime);
//  //我们添加进table 如果满了，重组
//  if (!inserter.Put(b.leafKey)) {
////    out("重组");
////    out(memNum + found);
//    int Nt = memNum + found;
//    int nt = max(Nt * Leaf_maxnum / Table_maxnum, Leaf_maxnum_rebalance);
//    inserter.Rebalance(nt, nt/2, Nt);
//  }
//
//  return Status();
//}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}


}  // namespace leveldb
