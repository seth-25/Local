//
// Created by hh on 2022/12/14.
//

#include "ST_merge.h"
#include "leveldb/cache.h"


namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};


ST_merge::ST_merge(VersionSet* ver, Compaction* c) : cache(ver->table_cache_), vec_size(0){
  ts_time tmpstart = 0x3f3f3f3f3f3f3f3f, tmpend = 0;
  for (auto & files : c->inputs_) {
    for (auto & file : files) {
      Cache::Handle* handle = nullptr;
      Status s = cache->FindTable(file->number, file->file_size, &handle);
      out2("afile");
      out2(file->number);
      out2(file->file_size);
      saxt_print(file->smallest.user_key().data());
      saxt_print(file->largest.user_key().data());
      if (s.ok()) {
        out("ok");
        Table* t = reinterpret_cast<TableAndFile*>(cache->cache_->Value(handle))->table;
//        out2("afile");
//        out2(file->number);
//        out2(file->file_size);
        Table::ST_Iter* stIter = new Table::ST_Iter(t);
        st_iters.insert(stIter);
        vec_size++;
        assert(vec_size<=100);
        handles[stIter] = handle;
//        cache->cache_->Release(handle);
        //更新时间
        tmpstart = min(tmpstart, file->startTime);
        tmpend = max(tmpend, file->endTime);
      }
    }
  }
  c->startTime = tmpstart;
  c->endTime = tmpend;
//  out("st_iters");

//  cout<<"vec:"<<vec_size<<endl;

  int i = 0;
  for(auto item: st_iters) {
    item->setPrefix(vec[i].first);
    if (item->next(vec[i].first)) {
      vec[i].second = item;
//      out("vec[i].first");
//      saxt_print(vec[i].first.asaxt);
    }
    else {
      del(item);
    }
    i++;
  }

  /*
  for(auto item: st_iters) {
    LeafKey leafKey;
    if (item->next(leafKey)) {
//      out("进堆");
      heap.emplace(leafKey, item);
    }
    else {
      del(item);
    }
  }
   */

}

bool ST_merge::next(LeafKey& leafKey) {
//  out("进入next");

//
  if (vec_size) {
    int res = 0;
    leafKey = vec[0].first;
    for (int i=1;i<vec_size;i++) {
      if (leafKey > vec[i].first) leafKey = vec[i].first, res = i;
    }
    if (!vec[res].second->next(vec[res].first)) {
      out2("要删除"+ to_string(res));
      out2(vec_size);
      del(vec[res].second);
      vec_size--;
      for(int i=res;i<vec_size;i++) {
        vec[i] = vec[i+1];
      }
    }

    return true;
  }
  return false;




/*
  if (!heap.empty()){
//    out("没空");
    leafKey.Set(heap.top().first);
    auto item = heap.top().second;
    heap.pop();
    LeafKey leafKey1;
    if (item->next(leafKey1)) heap.emplace(leafKey1, item);
    else {
      del(item);
    }
//    out("有next");
    return true;
  }
  return false;
  */
}


void ST_merge::del(Table::ST_Iter* it) {
  cache->cache_->Release(handles[it]);
  handles.erase(it);
  st_iters.erase(it);
  delete it;
}


}
