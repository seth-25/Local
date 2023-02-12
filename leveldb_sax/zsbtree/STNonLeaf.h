//
// Created by hh on 2022/12/12.
//

#ifndef LEVELDB_STNONLEAF_H
#define LEVELDB_STNONLEAF_H


#include <globals.h>
#include "cstring"
#include "STpos.h"
#include "STkeyinfo.h"
class STNonLeaf {
 public:

  STNonLeaf(int num, cod co_d, size_t size);
  STNonLeaf(size_t size);
  void Set(int num, cod co_d, size_t size);
  void Setrep(const char* newrep);
  // snappy用
  void Setrep1(const char* newrep);
  void Setisleaf();
  void Setnewroom(size_t size);

  void Setprefix(saxt_only prefix, saxt stleafkey, cod noco_size);
  void Setprefix(saxt_only prefix1);
  ~STNonLeaf();

  inline void SetSaxt(saxt_only& dst, saxt saxt_) {
    dst = prefix;
    memcpy(dst.asaxt, saxt_, s_co_size);
  }


  cod Get_co_d(int i);
  int Getnum(int i);
  saxt Get_lsaxt(int i);

  saxt Get_rsaxt(int i);

  STpos Get_pos(int i);


  int num;
  cod co_d;
  cod co_size;
  cod s_co_size;
  cod lkey_size;
  cod pos_size;
  cod noco_size;
  bool isleaf;
  bool ismmap;
  size_t size;
  saxt_only prefix;
  char* rep;
};

#endif  // LEVELDB_STNONLEAF_H
