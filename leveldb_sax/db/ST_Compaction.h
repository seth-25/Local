//
// Created by hh on 2022/12/16.
//

#ifndef LEVELDB_ST_COMPACTION_H
#define LEVELDB_ST_COMPACTION_H

#include <include/leveldb/table_builder.h>

#include "leveldb/table.h"


namespace leveldb {



class ST_Compaction_Leaf {
 public:

  typedef struct leaf_leafkey_rep{
    LeafKey rep[Leaf_rebuildnum];
  } leaf_leafkey;

  typedef struct nonleaf_leafkey_rep{
    NonLeafKey rep[Leaf_maxnum];
  } nonleaf_leafkey;

  leaf_leafkey leafs[compaction_leaf_size];
  nonleaf_leafkey nonleafs[compaction_leaf_size];
#if qiehuan
  void add(NonLeafKey* nonLeafKey, long snap_add, void*& tocopy) {
    snap_add %= compaction_leaf_size;
    LeafKey* leafKeys = (LeafKey*)nonLeafKey->p;
    int num = nonLeafKey->num;
    char* tmpbuffer = (char*)leafs[snap_add].rep;
    tocopy = tmpbuffer;
    memcpy((char*)leafs[snap_add].rep, leafKeys, num * sizeof(LeafKey));
  }

#else
  void add(NonLeafKey* nonLeafKey, long snap_add, void*& tocopy, size_t& size_tocopy) {
    snap_add %= compaction_leaf_size;
    cod co_d = nonLeafKey->co_d;
    size_t co_saxt_size = co_d * sizeof(saxt_type);
    size_t noco_saxt_size = sizeof(LeafKey) - co_saxt_size;
    LeafKey* leafKeys = (LeafKey*)nonLeafKey->p;
    char* tmpbuffer = (char*)leafs[snap_add].rep;
    tocopy = tmpbuffer;
    int num = nonLeafKey->num;
    size_tocopy = num * noco_saxt_size;
    //把共享的压缩掉
    for(int i=0;i<num;i++){
//      if(i>0 && leafKeys[i] < leafKeys[i-1]) {
//        out(1);
//        saxt_print(leafKeys[i].asaxt);
//        saxt_print(leafKeys[i-1].asaxt);
//        sleep(10);
//        exit(1);
//      }
//      if(i > 0) assert(leafKeys[i] >= leafKeys[i-1]);
      charcpy(tmpbuffer, leafKeys + i, noco_saxt_size);
    }
  }

  void add1(NonLeafKey* nonLeafKey, bool isleaf, long snap_add, void*& tocopy, size_t& size_tocopy) {
    snap_add %= compaction_leaf_size;
    cod co_d = nonLeafKey->co_d;
    size_t co_saxt_size = co_d * sizeof(saxt_type);
    size_t noco_saxt_size = sizeof(saxt_only) - co_saxt_size;
    NonLeafKey* nonLeafKeys = (NonLeafKey*)nonLeafKey->p;
    char* tmpbuffer = (char*)nonleafs[snap_add].rep;
    tocopy = tmpbuffer;
    int num = nonLeafKey->num;
    size_tocopy = num * (noco_saxt_size * 2 + sizeof(STkeyinfo) + sizeof(void*)) + 1;
    for(int i=0;i<nonLeafKey->num;i++){
      //    if (i>0 && nonLeafKeys[i].lsaxt < nonLeafKeys[i-1].rsaxt) {
      //      assert(nonLeafKeys[i].lsaxt >= nonLeafKeys[i-1].rsaxt);
      //    }
      NonLeafKey* nonLeafKey1 = nonLeafKeys + i;
      STkeyinfo stkeyinfo(nonLeafKey1->co_d, nonLeafKey1->num);
      charcpy(tmpbuffer, &stkeyinfo, sizeof(STkeyinfo));
      charcpy(tmpbuffer, &(nonLeafKey1->p), sizeof(void*));
      charcpy(tmpbuffer, nonLeafKey1->lsaxt.asaxt, noco_saxt_size);
      charcpy(tmpbuffer, nonLeafKey1->rsaxt.asaxt, noco_saxt_size);
    }
    charcpy(tmpbuffer, &isleaf, 1);
  }
#endif
};


class ST_Compaction : public Zsbtree_Build {
  void doleaf(NonLeafKey* nonLeafKey) override;
  void dononleaf(NonLeafKey* nonLeafKey, bool isleaf) override;

 public:
  ST_Compaction(int max_size, int min_size, TableBuilder* tableBuilder, ThreadPool* snap_pool, ST_Compaction_Leaf* leafs_);


  void add_root();

 private:
  ST_Compaction_Leaf* leafs_;
  TableBuilder* tableBuilder;
  ThreadPool* snap_pool;
  long snap_add;
};
}

#endif  // LEVELDB_ST_COMPACTION_H
