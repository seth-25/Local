//
// Created by hh on 2023/1/2.
//

#ifndef LEVELDB_QUERY_HEAP_H
#define LEVELDB_QUERY_HEAP_H

#include "globals.h"
#include "queue"
#include "mutex"
#include "boost/thread/shared_mutex.hpp"
#include "condition_variable"
#include "mem_version_set.h"
#include "version_set.h"
#include "unordered_set"

using namespace std;
namespace leveldb {

template <typename T>
class query_heap_rep {
 public:
  query_heap_rep(int k, int tableNum) : k(k), use(tableNum), over(tableNum+1), vv1(nullptr), vv2(nullptr){}

  ~query_heap_rep() {
    if (vv1 != nullptr) {
      v1_mutex->Lock();
      vv1->Unref();
      v1_mutex->Unlock();
    }
    if( vv2 != nullptr) {
      v2_mutex->Lock();
      vv2->Unref();
      v2_mutex->Unlock();
    }
  }

  //成功返回true
  bool push(T& T_) {
    if (res_heap.size() < k) {
      res_heap.push(T_);
      return true;
    } else if (T_ < res_heap.top()) {
      res_heap.pop();
      res_heap.push(T_);
      return true;
    }
    return false;
  }

  void get(vector<T>& res) {
    res.reserve(res_heap.size());
    while (!res_heap.empty()) {
      res.push_back(res_heap.top());
      res_heap.pop();
    }
  }

  int need() const { return k - res_heap.size(); }

  bool isk() const { return res_heap.size() == k; }

  virtual float top() const =0;


  inline void Lock() { mu_.lock(); }

  inline void Unlock() { mu_.unlock(); }


  inline void subUse() { use--; }

  // 这个表结束了
  inline bool subOver() {
    over--;
    if (over==1) {
      cv.notify_one();
    } else if (over==0){
      return true;
    }
    return false;
  }

  void wait() {
    std::unique_lock<std::mutex> l(mu_, std::adopt_lock);
    cv.wait(l);
    l.release();
  }

  bool isfinish() {
#if istime
    if (!use && res_heap.size() == k) {
      cv.notify_one();
      return true;
    }
    return false;
#else
    if (!use) {
      cv.notify_one();
      return true;
    }
    return false;
#endif
  }

  void sort_dist_p() {
    sort(to_sort_dist_p.begin(), to_sort_dist_p.end());
  }

 protected:
  int k;
  priority_queue<T> res_heap;
  int use;   // 有多少个表没做完第一个叶结点
  int over;  // 有多少个表没结束
  std::mutex mu_;
  std::condition_variable cv;




 public:
  // 记录版本和对应锁
  port::Mutex* v1_mutex;
  port::Mutex* v2_mutex;
  mem_version* vv1;
  Version* vv2;

//存下界距离
  vector<pair<float, void*>> to_sort_dist_p;

#if iscount_saxt_for_exact
  std::mutex mutex_saxt_num;
  uint64_t saxt_num_exact = 0;
#endif
};


class query_heap : public query_heap_rep<ares> {
 public:
  query_heap(int k, int tableNum)
      : query_heap_rep(k,tableNum) {}

  float top() const override {
    if (res_heap.empty()) return MAXFLOAT;
    return res_heap.top().rep.dist;
  }
};

class query_heap_exact : public query_heap_rep<ares_exact> {
 public:
  query_heap_exact(int k, int tableNum)
      : query_heap_rep(k,tableNum) {}

  void init(const vector<ares>& appro_res) {
    for(auto item: appro_res) {
      res_heap.push(item.rep);
      p_set.insert(item.p);
    }
  }

  inline bool is_in_set(void* p) {
    return p_set.count(p);
  }

  float top() const override {
    if (res_heap.empty()) return MAXFLOAT;
    return res_heap.top().dist;
  }




 private:
  std::unordered_set<void*> p_set;
};



}

#endif  // LEVELDB_QUERY_HEAP_H