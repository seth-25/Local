//
// Created by hh on 2022/12/16.
//

#include "ST_Compaction.h"


namespace leveldb{

void ST_Conmpaction::doleaf(NonLeafKey* nonLeafKey) {
  tableBuilder->AddLeaf(nonLeafKey);
}

void ST_Conmpaction::dononleaf(NonLeafKey* nonLeafKey, bool isleaf) {
  out("存");
  out(isleaf);
  tableBuilder->AddNonLeaf(nonLeafKey, isleaf);
}

ST_Conmpaction::ST_Conmpaction(int max_size, int min_size,
                               TableBuilder* tableBuilder) : Zsbtree_Build(max_size, min_size), tableBuilder(tableBuilder){}

}

