//
// Created by hh on 2022/12/1.
//

#ifndef LEVELDB_ZSBTREE_TABLE_H
#define LEVELDB_ZSBTREE_TABLE_H

//#include "STLeaf.h"
//#include "STNonLeaf.h"
//#include "STkeyinfo.h"
//#include "STpos.h"
#include "zsbtree_Build.h"
//#include "zsbtree_Insert.h"
//#include "zsbtree_finder.h"
namespace leveldb {



class zsbtree_table {
 public:

  zsbtree_table();

  zsbtree_table(zsbtree_table &im);

  zsbtree_table(zsbtree_table_mem table_mem);

  ~zsbtree_table();

  //false 重组
  bool Insert(LeafKey& leafKey);

  void BuildTree(newVector<NonLeafKey>& nonLeafKeys);



  zsbtree_table_mem Rebalance(int tmp_leaf_maxnum, int tmp_leaf_minnum, int Nt);

  void LoadNonLeafKeys(vector<NonLeafKey>& nonLeafKeys);





  //叶子数量， drange重构时用
  int leafNum;
  //标记叶子结点是否被使用，主要用来看删除不删除叶子
  bool isleafuse = false;
  //复制im的会设为true，删除时会不一样
  bool iscopy = false;
  NonLeaf* root;

 private:
  void Rebalance_dfs(NonLeaf* nonLeaf, vector<LeafKey>& sortleafKeys);

  void LoadNonLeafKeys_dfs(NonLeaf* nonLeaf, vector<NonLeafKey>& nonLeafKeys);

  void CopyTree_dfs(NonLeaf* nonLeaf, NonLeaf* copyNonLeaf);

  void DelTree_dfs(NonLeaf* nonLeaf);

  void DelTree_dfs_2(NonLeaf* nonLeaf);
};

}
#endif  // LEVELDB_ZSBTREE_TABLE_H
