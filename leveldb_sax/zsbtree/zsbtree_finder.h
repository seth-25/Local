//
// Created by hh on 2022/12/11.
//

#ifndef LEVELDB_ZSBTREE_FINDER_H
#define LEVELDB_ZSBTREE_FINDER_H
#include "zsbtree_table.h"


// -1 0 1代表在点左边，中间，右边


class ZsbTree_finder{
 public:
#if istime == 2
  ZsbTree_finder(saxt_only saxt_, ts_time time1, ts_time  time2, ts_type* paa_) {
    leafkey = saxt_;
    startTime = time1;
    endTime = time2;
    memcpy(paa, paa_, sizeof(ts_type) * Segments);
  }
#else
  ZsbTree_finder(saxt_only saxt_, ts_type* paa_) {
    leafkey = saxt_;
    memcpy(paa, paa_, sizeof(ts_type) * Segments);
  }
#endif


  // 从根节点查到一个叶节点的上一层非叶节点
  void root_Get(NonLeaf &nonLeaf);

  // 根据非叶节点找到最合理的点，并把leafkey返回给res
  void find_One(LeafKey* res, int& res_num);

  void find_One(LeafKey* res, int& res_num, Leaf *leaf);

  // 找到一个结点不够，按paa排序
  void sort();

 private:

  void l_Get_NonLeaf(NonLeafKey &nonLeafKey);

  void r_Get_NonLeaf(NonLeafKey &nonLeafKey);

  inline void leaf_Get(Leaf *leaf, int id, LeafKey* res, int& res_num);

// 在nonLeaf的范围里, 才调用
  void nonLeaf_Get(NonLeaf &nonLeaf);


 public:
  vector<dist_p> has_cod;
  vector<void*> no_has_cod;
 private:
  saxt_only leafkey;
#if istime == 2
  ts_time startTime;
  ts_time endTime;
#endif
  ts_type paa[Segments];
  NonLeaf* to_find_nonleaf;
  int oneId;//叶子在非叶结点中的位置

};



#endif  // LEVELDB_ZSBTREE_FINDER_H
