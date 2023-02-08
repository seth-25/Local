//
// Created by hh on 2022/11/22.
//

#ifndef TODOZSBTREE_LEAFKEY_H
#define TODOZSBTREE_LEAFKEY_H

#include "sax.h"
#include "cstring"


class LeafKey {
public:

    LeafKey();
    LeafKey(saxt_only saxt_);
    LeafKey(saxt_only saxt_, void* p);
    LeafKey(saxt_only prefix, char* stleafkey, cod noco_size) {
      asaxt = prefix;
      memcpy(this, stleafkey, noco_size);
    }


    inline void Set1(saxt_only prefix) {
      asaxt = prefix;
    }

    inline void Set2(char* stleafkey, cod noco_size) {
      memcpy(this, stleafkey, noco_size);
    }

    void setAsaxt(saxt_only saxt_);

    bool operator< (const LeafKey& leafKey) const ;

    bool operator> (const LeafKey& leafKey) const ;

    bool operator<= (const LeafKey& leafKey) const ;

    bool operator>= (const LeafKey& leafKey) const ;

#if istime == 2
    ts_time keytime_;
#endif
    void* p;
    saxt_only asaxt;
};

#if istime == 1

typedef struct LeafTimeKey_rep{
  LeafKey leafKey;
  ts_time keytime;
  bool operator< (const LeafTimeKey_rep& leafTimeKey) const {
    return leafKey < leafTimeKey.leafKey;
  }
} LeafTimeKey;

#elif istime == 2

typedef struct LeafTimeKey_rep{
  LeafKey leafKey;

  #define keytime getKeytime()

  ts_time getKeytime() {
    return leafKey.keytime_;
  }

  bool operator< (const LeafTimeKey_rep& leafTimeKey) const {
    return leafKey < leafTimeKey.leafKey;
  }
} LeafTimeKey;


#else

typedef struct LeafTimeKey_rep{
  LeafKey leafKey;
  bool operator< (const LeafTimeKey_rep& leafTimeKey) const {
    return leafKey < leafTimeKey.leafKey;
  }
} LeafTimeKey;
#endif


#endif //TODOZSBTREE_LEAFKEY_H
