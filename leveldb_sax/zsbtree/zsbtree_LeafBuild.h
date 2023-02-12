//
// Created by hh on 2022/11/21.
//

#ifndef TODOZSBTREE_ZSBTREE_LEAFBUILD_H
#define TODOZSBTREE_ZSBTREE_LEAFBUILD_H


#include <globals.h>
#include "newVector.h"
#include "vector"
#include "sax.h"
#include "Leaf.h"
#include "NonLeaf.h"


using namespace std;

namespace leaf_method {



    inline saxt_only get_saxt_i(newVector<LeafKey> &leafKeys, int i);

//构造leaf和索引点
    inline void build_leaf_and_nonleafkey(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, int id,
                                          int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt);

//构造leaf和索引点,从中间平分
    inline void build_leaf_and_nonleafkey_two(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, int id,
                                              int num, cod co_d, saxt_only lsaxt, saxt_only rsaxt);

//给一个叶子结点加一些key
    inline void add_nonleafkey(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, int id,
                               int num, cod co_d, saxt_only rsaxt);

//给一个叶子结点加一些key,到大于n了，平分
    inline void split_nonleafkey(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, int id, int allnum,
                                 int num, cod co_d, saxt_only rsaxt);

//在method2中，遇到大于n的，分一下
    inline int getbestmid(newVector<LeafKey> &leafKeys, const int n, const int m, int id, int num, cod d1, saxt_only now_saxt, saxt_only tmplastsaxt);


    inline int get_new_end(newVector<LeafKey> &leafKeys, int l, int r, saxt_only saxt_, cod co_d);
    inline int get_new_end_1(newVector<LeafKey> &leafKeys, int l, int r, saxt_only saxt_, cod co_d);
//待考虑几个平分时分节点有很多d=8的情况
//批量构建while循环内, 2n个
    int buildtree_window(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, const int n, const int m);


    void buildtree_window_last(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, int allnum, const int n, const int m);

//批量构建树，后面是两个流
    void buildtree(newVector<LeafKey> &leafKeys, vector<NonLeafKey> &nonLeafKeys, const int n, const int m);
}




#endif //TODOZSBTREE_ZSBTREE_LEAFBUILD_H
