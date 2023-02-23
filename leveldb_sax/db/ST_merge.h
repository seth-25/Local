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

  inline bool next(LeafKey* leafKey, int &add_size) {
    mutex_q.Lock();
//    cout<<"主线程获取"+to_string(size_)+":"<<(void*)this<<endl;
    if(!size_ && !isover) cv_q.Wait();
    if(!size_) {
      mutex_q.Unlock();
      return false;
    }

    if(hh + size_ <= Leaf_rebuildnum ) {
      memcpy(leafKey, q + hh, sizeof(LeafKey) * size_);
      hh = (hh + size_) % Leaf_rebuildnum;
    } else {
      int size1 = Leaf_rebuildnum - hh;
      memcpy(leafKey, q + hh, sizeof(LeafKey) * size1);
      memcpy(leafKey + size1, q, sizeof(LeafKey) * (size_ - size1));
    }
    add_size = size_;
    hh = (hh + size_) % Leaf_rebuildnum;
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

  PII vec[100];
  unsigned short vec_size;
//  priority_queue<PII, vector<PII>, greater<PII> > heap;

  //要release
  unordered_map<Table::ST_Iter*, Cache::Handle*> handles;
  //要delete
  LeafKey q[Leaf_rebuildnum];
  int hh;
  int tt;
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

  typedef pair<saxt_only , Table::ST_Iter*> PII_saxt;
  typedef pair<int, int> PII;

  inline bool next1(int i) {
    auto& hh_size = hh_size_buffer[i];
    if (hh_size.first < hh_size.second) {
      vec_key[i] = key_buffer[i][hh_size.first++];
    } else {
      if(next2(i, hh_size)) {
        vec_key[i] = key_buffer[i][hh_size.first++];
      } else return false;
    }
    return true;
  }

  inline bool next2(int i, PII &hh_size) {
    hh_size.first = 0;
    return vec_[i]->next(key_buffer[i], hh_size.second);
  }




  ThreadPool* pool_compaction;
  LeafKey vec_key[pool_compaction_size];

  LeafKey key_buffer[pool_compaction_size][Leaf_rebuildnum];
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
