//
// Created by hh on 2022/12/16.
//

#include "ST_Compaction.h"


namespace leveldb{

void ST_Compaction::doleaf(NonLeafKey* nonLeafKey) {
//  tableBuilder->wait_snap_wan(snap_add);
//  tableBuilder->AddLeaf(nonLeafKey);

  tableBuilder->wait_leaf_buffer(snap_add);
  void* tocopy;
#if qiehuan
  leafs_->add(nonLeafKey, snap_add, tocopy);
  snap_pool->enqueue(std::bind(&TableBuilder::AddLeaf, tableBuilder, nonLeafKey, tocopy));
#else
  size_t size_tocopy;
  leafs_->add(nonLeafKey, snap_add, tocopy, size_tocopy);
  snap_pool->enqueue(std::bind(&TableBuilder::AddLeaf, tableBuilder, nonLeafKey, tocopy, size_tocopy));
#endif
  snap_add++;
}

void ST_Compaction::dononleaf(NonLeafKey* nonLeafKey, bool isleaf) {
  out("dononleaf");
  for(int i=1;i<nonLeafKey->num;i++){
    assert(((NonLeafKey*)(nonLeafKey->p))[i].lsaxt >= ((NonLeafKey*)(nonLeafKey->p))[i-1].rsaxt);
  }

  tableBuilder->wait_snap_wan(snap_add);
  snap_pool->enqueue(std::bind(&TableBuilder::AddNonLeaf, tableBuilder, nonLeafKey, isleaf));
  snap_add++;
  tableBuilder->wait_snap_wan(snap_add);
//  tableBuilder->AddNonLeaf(nonLeafKey, isleaf);
}

ST_Compaction::ST_Compaction(int max_size, int min_size,
                               TableBuilder* tableBuilder,
                               ThreadPool* snap_pool, ST_Compaction_Leaf* leafs_) : Zsbtree_Build(max_size, min_size), snap_add(0),
                                                                                   tableBuilder(tableBuilder), snap_pool(snap_pool),
                                                                                leafs_(leafs_){
}




void ST_Compaction::add_root() {
  tableBuilder->wait_snap_wan(snap_add);
  auto nonleafkey = GetRootKey();
//  snap_pool->enqueue(std::bind(&TableBuilder::AddRootKey, tableBuilder, nonleafkey));
  tableBuilder->AddRootKey(nonleafkey);
}


}

