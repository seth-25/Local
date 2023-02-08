//
// Created by hh on 2022/12/9.
//

#include "STkeyinfo.h"


STkeyinfo::STkeyinfo(cod co_d, int num) {
  co_d_num = co_d;
  co_d_num |= num<<5;
}


cod STkeyinfo::GetCo_d() {
  return co_d_num & 31;
}


int STkeyinfo::GetNum() {
  return co_d_num >> (unsigned int)5;
}
