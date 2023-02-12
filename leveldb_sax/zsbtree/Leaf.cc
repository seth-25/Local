//
// Created by hh on 2022/11/21.
//

#include "Leaf.h"
#include "NonLeafKey.h"


Leaf::Leaf(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, LeafKey *leafKeys) {
    this->num = num;
    this->co_d = co_d;
    lsaxt = lsaxt;
    rsaxt = rsaxt;
    memcpy(this->leafKeys, leafKeys, sizeof(LeafKey) * num);
}

void Leaf::add(LeafKey *leafKeys, int num) {
    memcpy(this->leafKeys + this->num, leafKeys, sizeof(LeafKey) * num);
    this->num += num;
}



void Leaf::setLeafKeys(LeafKey *leafKeys) {
    memcpy(this->leafKeys, leafKeys, sizeof(LeafKey) * num);
}

void Leaf::setLsaxt(saxt_only saxt_) {
    lsaxt = saxt_;
}

void Leaf::setRsaxt(saxt_only saxt_) {
    rsaxt = saxt_;
}

Leaf::Leaf() {

}


void Leaf::set(NonLeafKey& nonLeafKey) {
  num = nonLeafKey.num;
  co_d = nonLeafKey.co_d;
  lsaxt = nonLeafKey.lsaxt;
  rsaxt = nonLeafKey.rsaxt;
//  out("set");
//  saxt_print(lsaxt);
//  saxt_print(rsaxt);
}

Leaf::Leaf(NonLeafKey& nonLeafKey) {
  num = nonLeafKey.num;
  co_d = nonLeafKey.co_d;
  lsaxt = nonLeafKey.lsaxt;
  rsaxt = nonLeafKey.rsaxt;
}

void Leaf::sort(LeafKey* dst) {
  mempcpy(dst, leafKeys, sizeof(LeafKey) * num);
  std::sort(dst, dst + num);
}

void Leaf::sort() {
  std::sort(leafKeys, leafKeys + num);
}
