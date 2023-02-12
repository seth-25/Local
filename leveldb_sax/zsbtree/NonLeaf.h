//
// Created by hh on 2022/11/22.
//

#ifndef TODOZSBTREE_NONLEAF_H
#define TODOZSBTREE_NONLEAF_H


#include "NonLeafKey.h"


class NonLeaf {
public:


    NonLeaf(int num, cod co_d, bool isleaf, saxt_only lsaxt, saxt_only rsaxt, NonLeafKey *nonLeafKeys);
    //添加一个saxt，p
    void add(NonLeafKey &nonLeafKey);
    void add(NonLeafKey *nonLeafKeys, int num);
    void setLsaxt(saxt_only saxt_);
    void setRsaxt(saxt_only saxt_);
    //有几个
    int num = 0;
    //相聚度
    cod co_d;
    //代表下一个是不是叶子
    bool isleaf;
    //公共前缀
    saxt_only lsaxt;
    saxt_only rsaxt;
    //叶结点中的key元组
    NonLeafKey nonLeafKeys[Leaf_maxnum];
};


#endif //TODOZSBTREE_NONLEAF_H
