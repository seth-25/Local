// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

//#include <string>

//#include "db/dbformat.h"

//#include "leveldb/db.h"
//#include "mem_version_set.h"
#include "zsbtree/zsbtree_table.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;
class mems_todel;



#define starttime_init 0x3f3f3f3f3f3f3f3f
#define endtime_init 0
class MemTable {

 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  MemTable(mems_todel* memsTodel);
  MemTable(ts_time starttime, ts_time endtime, mems_todel* memsTodel);
  MemTable(MemTable* im);
  MemTable(zsbtree_table_mem table_mem, mems_todel* memsTodel);
  MemTable(zsbtree_table_mem table_mem, ts_time starttime, ts_time endtime, mems_todel* memsTodel);

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  ~MemTable();
  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() ;


  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
//  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
//  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  bool Add(LeafKey& key);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  inline NonLeaf* GetRoot() { return table_.root; }


  inline saxt_only Getlsaxt() { return table_.root->lsaxt; }
  inline saxt_only Getrsaxt() { return table_.root->rsaxt; }
  inline cod Getcod();
  MemTable* Rebalance(int tmp_leaf_maxnum, int tmp_leaf_minnum, int Nt);
  inline int GetleafNum() { return table_.leafNum; }
  void LoadNonLeafKeys(vector<NonLeafKey> &nonLeafKeys);

 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

//  struct KeyComparator {
//    const InternalKeyComparator comparator;
//    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
//    int operator()(const char* a, const char* b) const;
//  };

//  typedef SkipList<const char*, KeyComparator> Table;





//  KeyComparator comparator_;
//  int refs_;
  mems_todel* memsTodel;

 public:
  int refs_;
  //维护两个时间戳
  ts_time startTime;
  ts_time endTime;

//  Arena arena_;
  typedef zsbtree_table Table;
  //根结点
  Table table_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
