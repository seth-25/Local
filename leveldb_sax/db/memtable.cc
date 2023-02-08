// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "mem_version_set.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(mems_todel* memsTodel) :refs_(0),startTime(starttime_init),endTime(endtime_init),memsTodel(memsTodel){}

MemTable::MemTable(ts_time starttime, ts_time endtime, mems_todel* memsTodel) :refs_(0),startTime(starttime),endTime(endtime),memsTodel(memsTodel){}

MemTable::~MemTable() { assert(refs_ == 0);}


//待改
//size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

//int MemTable::KeyComparator::operator()(const char* aptr,
//                                        const char* bptr) const {
//  // Internal keys are encoded as length-prefixed strings.
//  Slice a = GetLengthPrefixedSlice(aptr);
//  Slice b = GetLengthPrefixedSlice(bptr);
//  return comparator.Compare(a, b);
//}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
// public:
//  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}
//
//  MemTableIterator(const MemTableIterator&) = delete;
//  MemTableIterator& operator=(const MemTableIterator&) = delete;
//
//  ~MemTableIterator() override = default;
//
//  bool Valid() const override { return iter_.Valid(); }
//  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
//  void SeekToFirst() override { iter_.SeekToFirst(); }
//  void SeekToLast() override { iter_.SeekToLast(); }
//  void Next() override { iter_.Next(); }
//  void Prev() override { iter_.Prev(); }
//  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
//  Slice value() const override {
//    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
//    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
//  }
//
//  Status status() const override { return Status::OK(); }
//
// private:
//  MemTable::Table::Iterator iter_;
//  std::string tmp_;  // For passing to EncodeKey
};



bool MemTable::Add(LeafKey& key) {
  //这里待改，返回false重组
  return table_.Insert(key);
}




cod MemTable::Getcod() { return table_.root->co_d; }
MemTable* MemTable::Rebalance(int tmp_leaf_maxnum, int tmp_leaf_minnum, int Nt) {
  return new MemTable(table_.Rebalance(tmp_leaf_maxnum, tmp_leaf_minnum, Nt), startTime, endTime, memsTodel);
}

void MemTable::LoadNonLeafKeys(vector<NonLeafKey>& nonLeafKeys) { table_.LoadNonLeafKeys(nonLeafKeys); }

MemTable::MemTable(MemTable* im):refs_(0),table_(im->table_),startTime(starttime_init),endTime(endtime_init),memsTodel(im->memsTodel){}
MemTable::MemTable(zsbtree_table_mem table_mem, ts_time starttime, ts_time endtime, mems_todel* memsTodel) :refs_(0),table_(table_mem), startTime(starttime), endTime(endtime),memsTodel(memsTodel){}
void MemTable::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    //删除一个表是有代价的，但是unref一个表一定是来自一个版本，可以延迟删除，放入一个db的删除vector
    memsTodel->push(this);
  }
}
MemTable::MemTable(zsbtree_table_mem table_mem, mems_todel* memsTodel):refs_(0),table_(table_mem),startTime(starttime_init),endTime(endtime_init),memsTodel(memsTodel) {}

}  // namespace leveldb
