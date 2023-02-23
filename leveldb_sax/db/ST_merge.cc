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
  }

  memset(hh_size_buffer, 0, sizeof(PII) * vec_size);

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


  out("vec_size"+to_string(vec_size));

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
      delete vec_[res];
      vec_size--;
      for(int i=res;i<vec_size;i++) {
        vec_[i] = vec_[i+1];
        hh_size_buffer[i] = hh_size_buffer[i + 1];
        key_buffer[i] = key_buffer[i+1];
      }
    }

    return true;
  }
  return false;

}


ST_merge_one::ST_merge_one(TableCache* cache_): cache(cache_), vec_size(0), size_(0), cv_q(&mutex_q), isover(false), to_write(0){

}

void ST_merge_one::del(Table::ST_Iter* it) {
  cache->cache_->Release(handles[it]);
  handles.erase(it);
  delete it;
}

void ST_merge_one::start() {
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


  LeafKey tmpkey;
  while(next1(tmpkey)) {
    mutex_q.Lock();
//    cout<<"非线程加：" + to_string(size_)+":"<<(void*)this<<endl;
    if(size_==compaction_buffer_size) {
      cv_q.Wait();
      assert(size_ == 0);
    }
    key_buffer[to_write][size_++] = tmpkey;
    cv_q.Signal();
    mutex_q.Unlock();
  }

  mutex_q.Lock();
  isover = true;
  cv_q.Signal();
  mutex_q.Unlock();

}

bool ST_merge_one::next1(LeafKey& leafKey) {

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

}

}
