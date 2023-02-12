//
// Created by hh on 2022/11/22.
//

#ifndef TODOZSBTREE_NONLEAFKEY_H
#define TODOZSBTREE_NONLEAFKEY_H

#include "Leaf.h"
#include "STpos.h"
#include "sax.h"

class NonLeafKey {
public:
    NonLeafKey();
    NonLeafKey(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, void *p);
    void setLsaxt(saxt_only saxt_);
    void setRsaxt(saxt_only saxt_);
    void Set(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, void *p);



    int num;

    cod co_d;
    void* p;
    saxt_only lsaxt;
    saxt_only rsaxt;
};


#endif //TODOZSBTREE_NONLEAFKEY_H
