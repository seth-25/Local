//
// Created by hh on 2022/12/9.
//

#ifndef LEVELDB_STPOS_H
#define LEVELDB_STPOS_H

#include <cstddef>
class STpos {
 public:

  STpos();

  STpos(unsigned short size, size_t offset);

  void Set(unsigned short size, size_t offset);

  void* Get();

  unsigned short GetSize();

  size_t GetOffset();

 private:
  size_t pos;
};

#endif  // LEVELDB_STPOS_H
