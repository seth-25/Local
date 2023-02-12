//
// Created by hh on 2022/12/28.
//

#ifndef LEVELDB_MEM_VERSION_SET_H
#define LEVELDB_MEM_VERSION_SET_H

#include <port/port_stdcxx.h>

#include <utility>

#include "memtable.h"
#include "unordered_map"


namespace leveldb {



static inline void delete_mems(const vector<MemTable*>& mems_todel) {
  for (auto item : mems_todel)
    delete item;
}

class mems_todel {
 public:

  void push(MemTable* memTable);

  vector<MemTable*> get();


 private:
  //访问reps_的锁
  port::Mutex mutex_;
  vector<MemTable*> reps_;
};

//创建这个不用ref
class mems_boundary {
 public:

  explicit mems_boundary(vector<saxt_only> &bounds1);

  vector<saxt_only> Get();

  void Ref();

  void Unref();

 private:
  int size;
  saxt_only* bounds_;
  int refs_;
};

//创建这个不用ref
class mem_version {
 public:
  mem_version(vector<MemTable*> new_mems, mems_boundary* boundary);

  void Ref();

  // Drop reference count.  Delete if no more references exist.

  void Unref();

  vector<MemTable*> Get();

  ~mem_version();

  int size;
  //这个版本的mems
  MemTable** mems;
  mems_boundary* boundary;
// private:
  int refs_;

};

// 存稳定的可查的版本
class mem_version_set {
 public:
  mem_version_set();


  mem_version* CurrentVersion();

  int CurrentVersionId();

  //查询,没有就是nullptr
  mem_version* OldVersion(int id);

  // 表满了会， 大重组会, 会上报
  int newversion(mem_version* memVersion);

  int newversion(MemTable* newMem, int memId);

  //小重组不改版本号，不上报, 但会重新传建包含重组表的所有版本，
  void newversion_small(MemTable* newMem, MemTable* oldMem, int memId);

  //这个只有master来控制删除
  void Unref(int id);

  void UnrefAll();

  void test_ref();

 private:
  //版本号和对应的版本
  unordered_map<int, mem_version*> versions;
  int currentid;
};


}


#endif  // LEVELDB_MEM_VERSION_SET_H
