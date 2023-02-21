//
// Created by hh on 2022/11/22.
//

#include "LeafKey.h"

#include "Cmp.h"

LeafKey::LeafKey(saxt_only saxt_) : asaxt(saxt_){}

void LeafKey::setAsaxt(saxt_only saxt_) {
    asaxt = saxt_;
}

LeafKey::LeafKey() {

}

LeafKey::LeafKey(saxt_only saxt_, void* p): asaxt(saxt_), p(p){}




bool LeafKey::operator<(const LeafKey& leafKey) const {
  return asaxt < leafKey.asaxt;
}
//static int x1 = 0;
bool LeafKey::operator>(const LeafKey& leafKey) const {
//  printf("%d\n",++x1);
  return asaxt > leafKey.asaxt;
}

bool LeafKey::operator<=(const LeafKey& leafKey) const {
  return asaxt <= leafKey.asaxt;
}

bool LeafKey::operator>=(const LeafKey& leafKey) const {
  return asaxt >= leafKey.asaxt;
}
