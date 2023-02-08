//
// Created by hh on 2022/12/14.
//

#ifndef LEVELDB_ST_MERGE_H
#define LEVELDB_ST_MERGE_H

#include "version_set.h"
#include "leveldb/table.h"
#include "table_cache.h"
#include "version_set.h"
#include "queue"
#include "unordered_map"
#include "unordered_set"
namespace leveldb{
//用于归并排序
class ST_merge {
 public:
  ST_merge(VersionSet* ver, Compaction* c);

  bool next(LeafKey& leafKey);

 private:

  void del(Table::ST_Iter* it);


  typedef pair<LeafKey, Table::ST_Iter*> PII;

  PII vec[30];
  unsigned short vec_size;
//  priority_queue<PII, vector<PII>, greater<PII> > heap;
  TableCache* cache;
  //要release
  unordered_map<Table::ST_Iter*, Cache::Handle*> handles;
  //要delete
  unordered_set<Table::ST_Iter*> st_iters;
};

}

#endif  // LEVELDB_ST_MERGE_H
