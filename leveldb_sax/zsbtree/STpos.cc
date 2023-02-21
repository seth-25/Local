//
// Created by hh on 2022/12/9.
//

#include "STpos.h"

STpos::STpos(unsigned int size, size_t offset) {
  pos = size;
  pos |= offset << 20;
}

unsigned int STpos::GetSize() {
  return pos & 0x3ffff;
}

size_t STpos::GetOffset() {
//  printf("stpos %zu", pos);
  return (unsigned long long)pos >> (unsigned int)20;
}

STpos::STpos() {}

void STpos::Set(unsigned int size, size_t offset) {
    pos = size;
    pos |= offset << 20;
}

void* STpos::Get() {
  return (void*)pos;
}
