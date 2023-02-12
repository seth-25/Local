//
// Created by hh on 2022/12/9.
//

#ifndef LEVELDB_STKEYINFO_H
#define LEVELDB_STKEYINFO_H

#include "zsbtree_table.h"

class STkeyinfo {

 public:
  STkeyinfo(cod co_d, int num);

  cod GetCo_d();

  int GetNum();
 private:
  //前5位co_d，后11位num,也就是说最大16和1024
  unsigned int co_d_num;
};

#endif  // LEVELDB_STKEYINFO_H
