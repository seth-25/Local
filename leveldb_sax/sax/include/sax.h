//
//  sax.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isaxlib_sax_h
#define isaxlib_sax_h

#include "config.h"
#include "globals.h"
#include "ts.h"
#include <cstring>


void sax_from_ts(ts_type *ts_in, sax_type *sax_out);
void saxt_from_ts(ts_type *ts_in, saxt_type *saxt_out);

void paa_saxt_from_ts(ts_type *ts_in, saxt_type *saxt_out, ts_type *paa);

void sax_print(sax_type *sax, int segments, int bit_cardinality);
void sax_print(sax_type *sax);
void saxt_print(saxt_type *saxt);
void saxt_print(saxt_only saxt);
void leafkey_print(void* saxt);
void saxt_print(saxt_type *saxt, saxt_type *prefix, cod co_d);
void printbin(unsigned long long n, int size);
void serial_printbin (unsigned long long n, int size);
int compare(const void *a, const void *b);
//saxt 只有co_d个
float minidist_paa_to_saxt(const float* paa, saxt saxt_, cod co_d);
float minidist_paa_to_isax(ts_type *paa, sax_type *sax, sax_type *sax_cardinalities,
                           sax_type max_bit_cardinality,
                           sax_type max_cardinality,
                           int number_of_segments,
                           int min_val, int max_val, float ratio_sqrt);
ts_type minidist_paa_to_isax_raw(ts_type *paa, sax_type *sax,
			       sax_type *sax_cardinalities,
			       sax_type max_bit_cardinality,
			       int max_cardinality,
			       int number_of_segments,
			       int min_val,
			       int max_val,
			       ts_type ratio_sqrt) ;

//分开
void paa_from_ts(ts_type *ts_in, ts_type *paa_out);
void sax_from_paa (ts_type *paa, sax_type *sax);
void saxt_from_sax(sax_type *sax_in, saxt_type *saxt_out);
void sax_from_saxt(saxt_type *saxt_in, sax_type *sax_out);



//获得两个saxt的相聚度和公共前缀
static inline cod get_co_d_from_saxt(saxt_only a, saxt_only b) {
  int d;
  for(d=Bit_cardinality-1; d>=0; d--){
    if (a.asaxt[d] != b.asaxt[d]) return Bit_cardinality-1-d;
  }
  return Bit_cardinality;
}
//获得两个saxt的相聚度和公共前缀,但有比较的起始d
static inline cod get_co_d_from_saxt(saxt_only a, saxt_only b, cod pre_d) {
  int d = Bit_cardinality- 1 - pre_d;
  for(; d>=0; d--){
    if (a.asaxt[d] != b.asaxt[d]) return Bit_cardinality-1-d;
  }
  return Bit_cardinality;
}
//比较在第d个段是否相等
static inline bool compare_saxt_d(saxt_only a, saxt_only b, cod d) {
  return a.asaxt[Bit_cardinality - d] == b.asaxt[Bit_cardinality - d];
}








#endif
