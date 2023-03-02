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


ST_merge::ST_merge(VersionSet* ver, Compaction* c, ThreadPool* pool_compaciton_) : vec_size(0), pool_compaction(pool_compaciton_){
//  memset(&last, 0, sizeof(LeafKey));

  TableCache* cache = ver->table_cache_;
  vector<PII_saxt> st_iters;
  unordered_map<Table::ST_Iter*, Cache::Handle*> handles;
  ts_time tmpstart = 0x3f3f3f3f3f3f3f3f, tmpend = 0;
  for (auto & files : c->inputs_) {
    for (auto & file : files) {
      Cache::Handle* handle = nullptr;
      Status s = cache->FindTable(file->number, file->file_size, &handle);
      out2("afile");
      out2(file->number);
      out2(file->file_size);
//      saxt_print(file->smallest.user_key().data());
//      saxt_print(file->largest.user_key().data());

      saxt_only lsaxt(file->smallest.user_key().data());
      if (s.ok()) {
        out("ok");
        Table* t = reinterpret_cast<TableAndFile*>(cache->cache_->Value(handle))->table;
//        out2("afile");
//        out2(file->number);
//        out2(file->file_size);
        Table::ST_Iter* stIter = new Table::ST_Iter(t);
        st_iters.emplace_back(lsaxt, stIter);
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

  sort(st_iters.begin(), st_iters.end());

  vec_size = min((int)st_iters.size(), pool_compaction_size);

  for(int i=0;i<vec_size;i++) {
    vec_[i] = new ST_merge_one(cache);
  }

  for(int i=0;i<st_iters.size();i++) {
    int j = i % vec_size;
    vec_[j]->st_iters.push_back(st_iters[i].second);
    vec_[j]->handles[st_iters[i].second] = handles[st_iters[i].second];
  }



  for(int i=0;i<vec_size;i++) {
    pool_compaction->enqueue(&start_ST_merge_one, vec_[i]);
    key_buffer[i] = &(vec_[i]->key_buffer[2]);
  }



  memset(hh, 0, sizeof(int) * vec_size);

  for(int i=0;i<vec_size;i++) {
    if (!next1(i)) {
      delete vec_[i];
      vec_size--;
      for(int j=i;j<vec_size;j++) {
        vec_[j] = vec_[j+1];
      }
      i--;
    }
  }
//  cout<<"ting"<<endl;

  out("vec_size总"+to_string(vec_size));


}

bool ST_merge::next(LeafKey& leafKey) {
//  out("进入next");
  //dwad
//
  if (vec_size) {
    int res = 0;
    leafKey = get_buffer_merge(0);
    for (int i=1;i<vec_size;i++) {
      if (leafKey > get_buffer_merge(i)) leafKey = get_buffer_merge(i), res = i;
    }
    if (!next1(res)) {
      out2("要删除"+ to_string(res));
      out2(vec_size);
      vec_size--;
      for(int i=res;i<vec_size;i++) {
        vec_[i] = vec_[i+1];
        hh[i] = hh[i + 1];
        key_buffer[i] = key_buffer[i+1];
      }
    }
//    assert(last <= leafKey);
//    last = leafKey;
//    saxt_print(leafKey.asaxt);
//    cout<<hh[0]<<endl;
//    cout<<vec_size<<endl;
    return true;
  }
  return false;

}


ST_merge_one::ST_merge_one(TableCache* cache_): cache(cache_), vec_size(0), cv_q(&mutex_q){
//  memset(&last, 0, sizeof(LeafKey));
//  memset(&last1, 0, sizeof(LeafKey));
  to_get.store(0, memory_order_release);
  is_can_get.store(false, memory_order_release);
  isover.store(false, memory_order_release);
  to_write_id = 0;
  to_get_id = 0;

}

void ST_merge_one::del(Table::ST_Iter* it) {
  cache->cache_->Release(handles[it]);
  handles.erase(it);
  delete it;
}

void ST_merge_one::start() {

  for(auto & i : key_buffer) i.reserve(compaction_buffer_size * 16);

  vec_size = st_iters.size();
  vec.reserve(vec_size);

  for(int i = 0;i<vec_size;i++) {
    st_iters[i]->setPrefix(vec[i].first);
    if (st_iters[i]->next(vec[i].first)) {
      vec[i].second = st_iters[i];
//      out("vec[i].first");
//      saxt_print(vec[i].first.asaxt);
    }
    else {
      del(st_iters[i]);
      vec_size--;
      for(int j=i;j<vec_size;j++) {
        st_iters[j] = st_iters[j+1];
      }
      i--;
    }
  }

  out("vec_size分"+to_string(vec_size));
//  exit(13);

  LeafKey tmpkey;
  vector<LeafKey>* key_vec = &key_buffer[0];

  while(next1(tmpkey)) {
//    out("add");
//    saxt_print(tmpkey.asaxt);
//    saxt_print(tmpkey.asaxt);
//  assert(last1 <= tmpkey);
//  last1 = tmpkey;
//    assert(to_get_id != (to_write_id + 1) % 3);
    key_vec->push_back(tmpkey);
//    cout<<"写"+to_string(to_write_id)<<endl;
//    cout<<"size_hou"+to_string(key_vec->size())<<endl;
//    cout<<"size0wri:"+to_string(key_buffer[0].size());
//    cout<<"size1wri:"+to_string(key_buffer[1].size());
//    cout<<"size2wri:"+to_string(key_buffer[2].size());
//    for(int i=0;i<key_vec->size()-1;i++) {
//      if ((*key_vec)[i] > (*key_vec)[i+1]) {
//        cout<<i<<" "<<i+1<<" "<<to_write_id<<endl;
//        saxt_print((*key_vec)[i].asaxt);
//        saxt_print((*key_vec)[i+1].asaxt);
//      }
//      assert((*key_vec)[i] <= (*key_vec)[i+1]);
//    }
    if(key_vec->size() >= compaction_buffer_size) {
      if (to_get.load(memory_order_acquire) == to_write_id) {
        to_write_id = (to_write_id + 1) % 3;
//        for(int i=0;i<key_vec->size()-1;i++) {
//          assert((*key_vec)[i] <= (*key_vec)[i+1]);
//        }
        key_vec = &key_buffer[to_write_id];
        key_vec->clear();
//        cout<<"写"+to_string(to_write_id)<<endl;
//        cout<<(void*)key_vec<<endl;
//        cout<<"size0wri:"+to_string(key_buffer[0].size());
//        cout<<"size1wri:"+to_string(key_buffer[1].size());
//        cout<<"size2wri:"+to_string(key_buffer[2].size())<<endl;
        is_can_get.store(true, memory_order_release);

      }
    }
  }


//  cout<<"out"<<endl;
//  cout<<"size:"+to_string(key_buffer[0].size());
//  cout<<"size:"+to_string(key_buffer[1].size());
//  cout<<"size:"+to_string(key_buffer[2].size())<<endl;

  mutex_q.Lock();
  isover.store(true, memory_order_relaxed);
  cv_q.Wait();
  mutex_q.Unlock();

//  cout<<"outdel"<<endl;
}

bool ST_merge_one::next1(LeafKey& leafKey) {

  if (vec_size) {
    int res = 0;
    leafKey = vec[0].first;
    for (int i=1;i<vec_size;i++) {
      if (leafKey > vec[i].first) leafKey = vec[i].first, res = i;
    }
//    out1("res", res);
//    saxt_print(leafKey.asaxt);
    if (!vec[res].second->next(vec[res].first)) {
      out2("要删除-"+ to_string(res));
      out2(vec_size);
      del(vec[res].second);
      vec_size--;
      for(int i=res;i<vec_size;i++) {
        vec[i] = vec[i+1];
      }
    }
//
//    if (last > tmpkey) {
//      out("cuole");
//      saxt_print(last.asaxt);
//      saxt_print(tmpkey.asaxt);
//      out(1111);
//    }
//    assert(last <= tmpkey);
//    last = tmpkey;
//    saxt_print(last.asaxt);
//    saxt_print(leafKey.asaxt);
//    if(last >= leafKey) {
//      out("laile");
//      saxt_print(last.asaxt);
//      saxt_print(leafKey.asaxt);
//    }
//    assert(last < leafKey);
//    last = leafKey;

    return true;
  }
  return false;

}

}
