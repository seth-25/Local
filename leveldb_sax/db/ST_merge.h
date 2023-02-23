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

//多线程合并
class ST_merge_one {

 public:
  ST_merge_one(TableCache* cache_);


  void start();

  inline bool next(LeafKey*& leafKeys, int &add_size) {
    mutex_q.Lock();
//    cout<<"主线程获取"+to_string(size_)+":"<<(void*)this<<endl;
    if(!size_ && !isover) cv_q.Wait();
    if(!size_) {
      mutex_q.Unlock();
      return false;
    }
    leafKeys = key_buffer[to_write];
    add_size = size_;
    to_write = 1 - to_write;
    size_ = 0;
    cv_q.Signal();
    mutex_q.Unlock();
    return true;
  }

 private:

  bool next1(LeafKey& leafKey);

  void del(Table::ST_Iter* it);


  typedef pair<LeafKey, Table::ST_Iter*> PII;
  TableCache* cache;

 public:

  vector<Table::ST_Iter*> st_iters;


  vector<PII> vec;
  unsigned short vec_size;
//  priority_queue<PII, vector<PII>, greater<PII> > heap;

  //要release
  unordered_map<Table::ST_Iter*, Cache::Handle*> handles;

  
  LeafKey key_buffer[2][compaction_buffer_size];
  int to_write;
  //要delete
  int size_;
  bool isover;
  port::Mutex mutex_q;
  port::CondVar cv_q;
};

//用于归并排序
class ST_merge {
 public:
  ST_merge(VersionSet* ver, Compaction* c, ThreadPool* pool_compaction_);

  bool next(LeafKey& leafKey);

 private:

#define get_buffer_merge(i) key_buffer[i][hh_size_buffer[i].first]

  typedef pair<saxt_only , Table::ST_Iter*> PII_saxt;
  typedef pair<int, int> PII;

  inline bool next1(int i) {
    auto& hh_size = hh_size_buffer[i];
    hh_size.first++;
    return !(hh_size.first >= hh_size.second && !next2(i, hh_size));
  }

  inline bool next2(int i, PII &hh_size) {
    hh_size.first = 0;
    return vec_[i]->next(key_buffer[i], hh_size.second);
  }




  ThreadPool* pool_compaction;
//  LeafKey vec_key[pool_compaction_size];

  LeafKey* key_buffer[pool_compaction_size];
  PII hh_size_buffer[pool_compaction_size];


  unsigned short vec_size;
  //要delete
  ST_merge_one* vec_[pool_compaction_size];
};



static void start_ST_merge_one(ST_merge_one* one){
  one->start();
}


}

#endif  // LEVELDB_ST_MERGE_H
