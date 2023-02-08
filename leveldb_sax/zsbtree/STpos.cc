//
// Created by hh on 2022/12/9.
//

#include "STpos.h"


STpos::STpos(unsigned short size, size_t offset) {
  pos = size;
  pos |= offset << 16;
}

unsigned short STpos::GetSize() {
  return *(unsigned short*)(&pos);
}

size_t STpos::GetOffset() {
  return (unsigned long long)pos >> (unsigned short)16;
}

STpos::STpos() {}

void STpos::Set(unsigned short size, size_t offset) {
    pos = size;
    pos |= offset << 16;
}

void* STpos::Get() {
  return (void*)pos;
}
