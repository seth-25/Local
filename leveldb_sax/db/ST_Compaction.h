//
// Created by hh on 2022/12/16.
//

#ifndef LEVELDB_ST_COMPACTION_H
#define LEVELDB_ST_COMPACTION_H

#include <include/leveldb/table_builder.h>

#include "leveldb/table.h"

namespace leveldb {
class ST_Conmpaction : public Zsbtree_Build {
  void doleaf(NonLeafKey* nonLeafKey) override;
  void dononleaf(NonLeafKey* nonLeafKey, bool isleaf) override;

 public:
  ST_Conmpaction(int max_size, int min_size, TableBuilder* tableBuilder);

 private:
  TableBuilder* tableBuilder;
};
}

#endif  // LEVELDB_ST_COMPACTION_H
