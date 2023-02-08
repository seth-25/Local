//
// Created by hh on 2022/12/28.
//

#include "mem_version_set.h"

#include <utility>


namespace leveldb {


void mems_todel::push(MemTable* memTable) {
  mutex_.Lock();
  reps_.push_back(memTable);
  mutex_.Unlock();
}

vector<MemTable*> mems_todel::get() {
  mutex_.Lock();
  vector<MemTable*> res = reps_;
  reps_.clear();
  mutex_.Unlock();
  return res;
}


//创建这个不用ref

void mems_boundary::Ref() { ++refs_; }

void mems_boundary::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    free(bounds_);
    delete this;
  }
}
mems_boundary::mems_boundary(vector<saxt_only>& bounds1) :refs_(0) {
    size = bounds1.size();
    bounds_ = (saxt_only*)malloc(sizeof(saxt_only)*size);
    memcpy(bounds_, bounds1.data(), sizeof(saxt_only)*size);
}

vector<saxt_only> mems_boundary::Get() {
  vector<saxt_only> res;
  res.reserve(size);
  for(int i=0;i<size;i++) res.push_back(bounds_[i]);
  return res;
}

mem_version::mem_version(vector<MemTable*> new_mems, mems_boundary* boundary)
    : refs_(0), size(new_mems.size()), boundary(boundary) {
  boundary->Ref();
  mems = (MemTable**)malloc(sizeof(void*)*size);
  memcpy(mems, new_mems.data(), sizeof(void*)*size);
  for (int i=0;i<size;i++) mems[i]->Ref();
}

void mem_version::Ref() { ++refs_; }

// Drop reference count.  Delete if no more references exist.

void mem_version::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    delete this;
  }
}

mem_version::~mem_version() {
  for (int i=0;i<size;i++) mems[i]->Unref();
  boundary->Unref();
  free(mems);
}

vector<MemTable*> mem_version::Get() {
  vector<MemTable*> res;
  res.reserve(size);
  for(int i=0;i<size;i++) res.push_back(mems[i]);
  return res;
}

mem_version_set::mem_version_set() : currentid(0){}

//插入
mem_version* mem_version_set::CurrentVersion() { return versions[currentid]; }

//查询,没有就是nullptr
mem_version* mem_version_set::OldVersion(int id) { return versions[id]; }

// 表满了会， 大重组会, 会上报
int mem_version_set::newversion(mem_version* memVersion) {
  memVersion->Ref();
  currentid++;
  versions[currentid] = memVersion;
  return currentid;
}

int mem_version_set::newversion(MemTable* newMem, int memId) {
  
  mem_version* current = versions[currentid];
  vector<MemTable*> newVec = current->Get();
  newVec[memId] = newMem;
  mem_version* newVersion = new mem_version(newVec, current->boundary);
  newVersion->Ref();
  currentid++;
  versions[currentid] = newVersion;

  return currentid;

}


void mem_version_set::newversion_small(MemTable* newMem, MemTable* oldMem, int memId) {
//  out(oldMem->refs_);
//  out(newMem->refs_);
  // 遍历所有版本
  bool isunref = true;
  for(auto item: versions) {
    if (item.second->mems[memId] == oldMem) {
//      out("laile");
      isunref = false;
      vector<MemTable*> v = item.second->Get();

//      out("ref");
//      for(int i=0;i<v.size();i++) {
//        out(v[i]->refs_);
//      }


      v[memId] = newMem;
      mem_version* newversion = new mem_version(v, item.second->boundary);
      newversion->Ref();
      item.second->Unref();
      versions[item.first] = newversion;
    }
  }
  if (isunref) {
//    out(oldMem->refs_);
//    out(newMem->refs_);
    oldMem->Unref();
    newMem->Ref();
  }
}

//这个只有master来控制删除
void mem_version_set::Unref(int id) {
  versions[id]->Unref();
  versions.erase(id);
}

int mem_version_set::CurrentVersionId() { return currentid; }

void mem_version_set::UnrefAll() {
  for(auto item: versions) {
    item.second->Unref();
  }
}

void mem_version_set::test_ref() {
  for(auto item: versions) {
    if (item.second->refs_<=0) out(item.first);
    assert(item.second->refs_>0);
  }
}


}