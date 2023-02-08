//
// Created by hh on 2022/11/22.
//

#include "NonLeaf.h"



NonLeaf::NonLeaf(int num, cod co_d, bool isleaf, saxt_only lsaxt, saxt_only rsaxt, NonLeafKey *nonLeafKeys) {
    this->num = num;
    this->co_d = co_d;
    this->isleaf = isleaf;
    this->lsaxt = lsaxt;
    this->rsaxt = rsaxt;
    memcpy(this->nonLeafKeys, nonLeafKeys, sizeof(NonLeafKey) * num);
}


void NonLeaf::add(NonLeafKey &nonLeafKey) {
    memcpy(nonLeafKeys + num, &nonLeafKey, sizeof(NonLeafKey));
    num++;
}

void NonLeaf::add(NonLeafKey *nonLeafKeys, int num) {
    memcpy(this->nonLeafKeys + this->num, nonLeafKeys, sizeof(NonLeafKey) * num);
    this->num += num;
}


void NonLeaf::setLsaxt(saxt_only saxt_) {
    lsaxt = saxt_;
}

void NonLeaf::setRsaxt(saxt_only saxt_) {
    rsaxt = saxt_;
}




