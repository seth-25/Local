//
// Created by hh on 2022/12/12.
//

#ifndef LEVELDB_STLEAF_H
#define LEVELDB_STLEAF_H

#include <globals.h>
#include "cstring"
#include "LeafKey.h"

class STLeaf {
 public:

  STLeaf(int num, cod co_d, size_t size);
  STLeaf(size_t size);

  void Setrep(const char* newrep);
  // snappy用
  void Setrep1(const char* newrep);
  void Setnewroom(size_t size);
  void Set(int num, cod co_d);
  void Setprefix(saxt_only prefix, saxt stleafkey, cod noco_size);
  void Setprefix(saxt_only prefix1);
  inline void SetLeafKey(LeafKey* dst, int id) {
    dst->asaxt = prefix;
    memcpy(dst, Get_rep(id), noco_size);
  }
  ~STLeaf();

  char* Get_rep(int i);

  int num;
  cod co_d;
  cod co_size;
  cod noco_size;
  bool ismmap;
  saxt_only prefix;
  char* rep;

};

#endif  // LEVELDB_STLEAF_H
