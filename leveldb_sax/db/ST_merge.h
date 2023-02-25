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

  inline bool next(vector<LeafKey>*& leafKeys) {
//    cout<<"get"<<endl;
    // 先清理

//    cout<<"size0:"+to_string(key_buffer[0].size());
//    cout<<"size1:"+to_string(key_buffer[1].size());
//    cout<<"size2:"+to_string(key_buffer[2].size());


    while (!is_can_get.load(memory_order_acquire)) {
      if (isover.load(memory_order_relaxed)) {
        leafKeys->clear();
        if (!key_buffer[to_get_id].empty()) {
          leafKeys = &(key_buffer[to_get_id]);
          return true;
        }
        mutex_q.Lock();
        cv_q.Signal();
        mutex_q.Unlock();
        return false;
      }
    }



//    cout<<"使用"+to_string(to_get_id)<<endl;
    leafKeys = &key_buffer[to_get_id];
//    cout<<"size_shiyong222222"+to_string(key_buffer[to_get_id].size())<<endl;
//    cout<<"size_shiyong"+to_string(leafKeys->size())<<endl;
    to_get_id = (to_get_id + 1) % 3;
    is_can_get.store(false, memory_order_relaxed);
    to_get.store(to_get_id, memory_order_release);
//    cout<<"to_get_id"+to_string(to_get_id)<<endl;
//    for(int i=0;i<(*leafKeys).size()-1;i++) {
//      if((*leafKeys)[i] > (*leafKeys)[i+1]) {
//        cout<<i<<" "<<i+1<<" "<<to_get_id-1<<endl;
//        saxt_print((*leafKeys)[i].asaxt);
//        saxt_print((*leafKeys)[i+1].asaxt);
//      }
//      assert((*leafKeys)[i] <= (*leafKeys)[i+1]);
//    }


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


  vector<LeafKey> key_buffer[3];
  
//  LeafKey key_buffer[3][compaction_buffer_size];

  atomic<int> to_get; // 下一个要获取的id
  atomic<bool> is_can_get; // 已经准备好了

  int to_get_id; //取的线程单独用
  int to_write_id; // 写的线程单独用，在写第几组


  atomic<bool> isover;
  port::Mutex mutex_q;
  port::CondVar cv_q;
};

//用于归并排序
class ST_merge {

 public:
  ST_merge(VersionSet* ver, Compaction* c, ThreadPool* pool_compaction_);

  bool next(LeafKey& leafKey);

 private:

#define get_buffer_merge(i) (*(key_buffer[i]))[hh[i]]

  typedef pair<saxt_only , Table::ST_Iter*> PII_saxt;


  inline bool next1(int i) {
    hh[i]++;
    return !(hh[i] >= (*(key_buffer[i])).size() && !next2(i));
  }

  inline bool next2(int i) {
    hh[i] = 0;
    return vec_[i]->next(key_buffer[i]);
  }




  ThreadPool* pool_compaction;
//  LeafKey vec_key[pool_compaction_size];

  vector<LeafKey>* key_buffer[pool_compaction_size];
  int hh[pool_compaction_size];



  unsigned short vec_size;
  //要delete
  ST_merge_one* vec_[pool_compaction_size];
};



static void start_ST_merge_one(ST_merge_one* one){
  one->start();
  delete one;
}


}

#endif  // LEVELDB_ST_MERGE_H
