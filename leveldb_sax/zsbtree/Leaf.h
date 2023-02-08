//
// Created by hh on 2022/11/21.
//

#ifndef TODOZSBTREE_LEAF_H
#define TODOZSBTREE_LEAF_H

#include <cstring>

#include "Cmp.h"
#include "LeafKey.h"
//#include "NonLeafKey.h"
#include "algorithm"

class NonLeafKey;

class Leaf {
public:
    Leaf();
    Leaf(int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt, LeafKey *leafKeys);
    Leaf(NonLeafKey& nonLeafKey);
    //添加一个saxt，p
    void add(LeafKey *leafKeys, int num);
    void setLeafKeys(LeafKey *leafKeys);
    void setLsaxt(saxt_only saxt_);
    void setRsaxt(saxt_only saxt_);
    //先复制然后在复制的内存中排序
    void sort(LeafKey* dst);
    void sort();
    void set(NonLeafKey& nonLeafKey);
    //有几个
    int num = 0;
    //相聚度
    cod co_d;
    saxt_only lsaxt;
    saxt_only rsaxt;
    //叶结点中的key元组
    LeafKey leafKeys[Leaf_rebuildnum];

    inline void add(LeafKey *leafKey) {
      memcpy(this->leafKeys + this->num, leafKey, sizeof(LeafKey));
      num++;
    }
};


#endif //TODOZSBTREE_LEAF_H
