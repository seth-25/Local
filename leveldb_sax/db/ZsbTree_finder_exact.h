//
// Created by hh on 2023/1/17.
//

#ifndef LEVELDB_ZSBTREE_FINDER_EXACT_H
#define LEVELDB_ZSBTREE_FINDER_EXACT_H

#include <jni/send_master.h>

#include "query_heap.h"
#include "zsbtree_table.h"
namespace leveldb {
class ZsbTree_finder_exact {
 public:

  ZsbTree_finder_exact(const aquery& aquery_, query_heap_exact* heap_, NonLeaf* root, const void* gs_jvm): aquery1(aquery_), heap(heap_), root(root), gs_jvm(gs_jvm) {
    charcpy(add_info, &aquery1.rep, sizeof(aquery_rep));
    charcpy(add_info, &aquery1.k, sizeof(int));
    tmp_distp.reserve(Leaf_rebuildnum);
    tmp_distp.resize(Leaf_rebuildnum);
  }


  void start();

  ~ZsbTree_finder_exact() {
    free(info);
  }

 protected:
  virtual void leaf_todo(Leaf& leaf);

  void nonLeaf_dfs(NonLeaf& nonLeaf);

  const aquery& aquery1;
  query_heap_exact* heap;
  NonLeaf* root;
  float topdist;
  const void* gs_jvm;
  int need;
  char* info = (char*)malloc(sizeof(aquery_rep) + sizeof(int)*3 + sizeof(float) + sizeof(void*) * Leaf_rebuildnum);
  char* add_info = info;
  vector<pair<float, void*>> tmp_distp;

#if iscount_saxt_for_exact
 public:
  uint64_t saxt_num = 0;
#endif
};

class ZsbTree_finder_exact_appro: public ZsbTree_finder_exact {
 public:
  ZsbTree_finder_exact_appro(const aquery& aquery, query_heap_exact* heap,
                             NonLeaf* root, const void* gsJvm)
      : ZsbTree_finder_exact(aquery,heap,root,gsJvm) {}

 private:
  void leaf_todo(Leaf& leaf) override;
};

}

#endif  // LEVELDB_ZSBTREE_FINDER_EXACT_H
