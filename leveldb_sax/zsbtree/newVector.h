//
// Created by hh on 2022/12/2.
//

#ifndef LEVELDB_NEWVECTOR_H
#define LEVELDB_NEWVECTOR_H


#include "cstring"
#include "vector"
#include "globals.h"

template <typename T>
class newVector {
 public:

  newVector(): l(0), r(0), topush_pos(0), size_(0){};

  newVector(T* v, int l, int r) {
    this->l = v + l;
    this->r = v + r;
    size_ = r - l;
    topush_pos = this->l;
  }

  newVector(newVector<T>& v, int l, int r) {
      this->l = v.l + l;
      this->r = v.l + r;
      size_ = r - l;
      topush_pos = this->l;
  }

  newVector(std::vector<T>& v) {

      this->l = v.data();
      this->r = v.data() + v.size();
      size_ = v.size();
      topush_pos = l;
  }

  newVector(std::vector<T>& v, int l, int r) {
      this->l = v.data() + l;
      this->r = v.data() + r;
      size_ = r - l;
      topush_pos = this->l;
  }

  void Set(T* v, int ll, int rr) {
    l = v + ll;
    r = v + rr;
    size_ = rr - ll;
    topush_pos = l;
  }

  void Set(std::vector<T>& v, int ll, int rr) {
      l = v.data() + ll;
      r = v.data() + rr;
      size_ = rr - ll;
      topush_pos = l;
  }

  T* data() const {
      return l;
  }

  T* begin() {
      return l;
  }

  T* end() {
      return r;
  }

  T& back() {
      return *(r - 1);
  }

  size_t size() {return size_;}

  T& operator[] (size_t n) const {
      return *(l + n);
  }

  void resize(size_t size) {
      topush_pos = l + size;
  }

  void restart(T* ll) {
      l = ll;
      r = ll + size_;
  }

  bool push_back(T& vv) {
      memcpy(topush_pos, &vv, sizeof(T));
      topush_pos++;
      return topush_pos != r;
  }

  size_t size_add() const {
      return topush_pos - l;
  }

  T* back_add() const {
      return topush_pos - 1;
  }

  bool empty_add() {
      return topush_pos == l;
  }

  void posadd(){
    topush_pos++;
  }

  bool isfull() {
    return topush_pos == r;
  }



  T* topush_pos;
  T* l;
  T* r;
  size_t size_;

};

template <typename T>
class jniVector {
 public:

  jniVector(T* data_, size_t size_) : data_(data_), size_(size_) {}


  inline T* data() const {
    return data_;
  }

  size_t size() const {return size_;}

  T& operator[] (size_t n) const {
    return *(data_ + n);
  }

  void push_back(const T& vv) {
    memcpy(data_ + size_, &vv, sizeof(T));
    size_++;
  }

  bool empty() { return size_; }



  T* data_;
  size_t size_;

};


typedef struct jniInfo_rep {
  char* info_p;
  void* bytebuffer_p;
} jniInfo;





#endif  // LEVELDB_NEWVECTOR_H
