//
// Created by hh on 2022/12/12.
//

#include "STNonLeaf.h"


STNonLeaf::STNonLeaf(int num, cod co_d, size_t size) {
  this->num = num;
  assert(co_d <=8);
  this->co_d = co_d;
  co_size = co_d * sizeof(saxt_type);
  lkey_size = sizeof(saxt_only) - co_size;
  s_co_size = lkey_size;
  pos_size = (lkey_size << 1) + sizeof(int);
  lkey_size += sizeof(int) + sizeof(void*);
  noco_size = pos_size + sizeof(void*);
  rep = new char[size];
  this->size = size;
  isleaf = false;
  ismmap = false;
}

STNonLeaf::STNonLeaf(size_t size) {
  rep = new char[size];
  ismmap = false;
  isleaf = false;
}

void STNonLeaf::Set(int num, cod co_d, size_t size) {
  assert(co_d <=8);
  this->num = num;
  this->co_d = co_d;
  co_size = co_d * sizeof(saxt_type);
  lkey_size = sizeof(saxt_only) - co_size;
  s_co_size = lkey_size;
  noco_size = (lkey_size << 1) + sizeof(void*) + sizeof(int);
  lkey_size += sizeof(void*) + sizeof(int);
  this->size = size;
}

void STNonLeaf::Setrep(const char* newrep) {

  if (newrep!=rep){
    if (!ismmap)
      delete[] rep;
    ismmap = true;
    rep = const_cast<char*>(newrep);
  }
}

void STNonLeaf::Setisleaf() { isleaf = *(bool*)(rep + size - 1); }

STNonLeaf::~STNonLeaf() {
  if (!ismmap) delete[] rep;
}

cod STNonLeaf::Get_co_d(int i) {
  return ((STkeyinfo*)(rep + i * noco_size))->GetCo_d();
}
int STNonLeaf::Getnum(int i) {
  return ((STkeyinfo*)(rep + i * noco_size))->GetNum();
}
saxt STNonLeaf::Get_lsaxt(int i) { return (saxt)(rep + i * noco_size + sizeof(int) + sizeof(void*)); }
saxt STNonLeaf::Get_rsaxt(int i) { return (saxt)(rep + i * noco_size + lkey_size); }

STpos STNonLeaf::Get_pos(int i) {
  return *((STpos*)(rep + i * noco_size + sizeof(STkeyinfo)));
}

void STNonLeaf::Setnewroom(size_t size) {
  rep = new char[size];
  ismmap = false;
}

void STNonLeaf::Setprefix(saxt_only prefix, saxt stleafkey,
                          cod noco_size) {
  this->prefix = prefix;
  memcpy(this->prefix.asaxt, stleafkey, noco_size);
}
void STNonLeaf::Setprefix(saxt_only prefix1) {
  this->prefix = prefix1;
}

void STNonLeaf::Setrep1(const char* newrep) {
  if (!ismmap)
    delete[] rep;
  ismmap = false;
  rep = const_cast<char*>(newrep);
}
