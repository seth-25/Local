//
// Created by hh on 2022/11/22.
//

#include "NonLeafKey.h"



NonLeafKey::NonLeafKey(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, void *p) {
    this->num = num;
    this->co_d = co_d;
    this->lsaxt = lsaxt;
    this->rsaxt = rsaxt;
    this->p = p;
}


void NonLeafKey::setLsaxt(saxt_only saxt_) {
    lsaxt = saxt_;
}

void NonLeafKey::setRsaxt(saxt_only saxt_) {
    rsaxt = saxt_;
}

NonLeafKey::NonLeafKey() {

}

void NonLeafKey::Set(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, void* p) {
  this->num = num;
  this->co_d = co_d;
  this->lsaxt = lsaxt;
  this->rsaxt = rsaxt;
  this->p = p;
}


