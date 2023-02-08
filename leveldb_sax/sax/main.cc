//
// Created by MasterH on 2022/11/21.
//


#include "stdio.h"
#include "iostream"
#include "include/sax.h"
#include "include/ts.h"
#include "include/globals.h"
#include "bitset"


using namespace std;



int main(){
    ts_type a[] = {0,0,0,0,0.00001,0.00001,0.00001,0.00001};
    ts_type b[] = {-100,-100,-100,-100,0,0,0,0};
    ts_type *paa = (ts_type*) malloc(sizeof(ts_type)*8);
    sax_type *sax = (sax_type*) malloc(sizeof(sax_type)*8);
    saxt_type *saxt = (saxt_type*) malloc(sizeof(saxt_type)*8);

//    paa_from_ts(a, paa, 8, 1);
//    ts_print(paa, 8);
//    saxt_from_ts(a, saxt, 1, 8, 256, 8);
//    sax_from_saxt(saxt, sax, 8, 8);
////    sax_from_ts(a, sax, 1, 8, 256);
////    sax_print(sax, 8, 8);
////    saxt_from_sax(sax, saxt, 8, 8);
//    saxt_print(saxt, 8, 8);
//    sax_print(sax, 8, 8);

}



