//
//  sax.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
//#define DEBUG;
#include "../include/config.h"
#include "../include/globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#ifdef VALUES
	#include <values.h>
#include <iostream>

#endif

#include "../include/sax.h"
#include "../include/ts.h"
#include "../include/sax_breakpoints.h"
#include "algorithm"
#include "immintrin.h"
#include "sax_bsearch.h"
/** 
 This is used for converting to sax
 */
int compare(const void *a, const void *b)
{
    float * c = (float *) b - 1;
    if (*(float*)a>*(float*)c && *(float*)a<=*(float*)b) {
        //printf("Found %lf between %lf and %lf\n",*(float*)a,*(float*)c,*(float*)b);
        return 0;
    }
    else if (*(float*)a<=*(float*)c) {
        return -1;
    }
    else
    {
        return 1;
    }
}

/** 
 Calculate paa.
 */
void paa_from_ts (ts_type *ts_in, ts_type *paa_out) {

    __m256 xfsSum;
    __m256 a;
    for (int i=0; i<Segments; i++) {
      xfsSum = _mm256_setzero_ps();
      int off = i * Ts_values_per_segment;
      for (int j=0;j<Ts_values_per_segment;j+=8) {
        a = _mm256_loadu_ps(ts_in + off + j);
        xfsSum = _mm256_add_ps(xfsSum, a);
      }
      const auto* q = (const float*)&xfsSum;
      paa_out[i] = (q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7]) / Ts_values_per_segment;
    }
    
}


void sax_from_paa (ts_type *paa, sax_type *sax) {

    //printf("FROM %lf TO %lf\n", sax_breakpoints[offset], sax_breakpoints[offset + cardinality - 2]);

  __m256 vmask;
  __m256 vt;
  __m128 vmask1;

  for(int i=0;i<Segments;i++){

    vt = _mm256_set1_ps(paa[i]);
    vmask = _mm256_cmp_ps(vt, BM.a1, _CMP_GT_OQ);
    int bit_mask = _mm256_movemask_ps(vmask);

    int d = sax_c1[bit_mask];
    vmask = _mm256_cmp_ps(vt, BM.a2[d], _CMP_GT_OQ);
    bit_mask = _mm256_movemask_ps(vmask);

    int d1 = sax_c1[bit_mask];
    vmask1 = _mm_cmp_ps(*(__m128*)&vt, BM.a3[d][d1], _CMP_GT_OQ);
    bit_mask = _mm_movemask_ps(vmask1);

    sax[i] = (d << 5) + (d1 << 2) + sax_c1[bit_mask];
  }

  
}

/**
 This function converts a ts record into its sax representation.
 */
void sax_from_ts(ts_type *ts_in, sax_type *sax_out)
{
    // Create PAA representation
    float paa[Segments];


  __m256 xfsSum;
  __m256 a;
  for (int i=0; i<Segments; i++) {
    xfsSum = _mm256_setzero_ps();
    int off = i * Ts_values_per_segment;
    for (int j=0;j<Ts_values_per_segment;j+=8) {
      a = _mm256_loadu_ps(ts_in + off + j);
      xfsSum = _mm256_add_ps(xfsSum, a);
    }
    const auto* q = (const float*)&xfsSum;
    paa[i] = (q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7]) / Ts_values_per_segment;
  }
    
    // Convert PAA to SAX
    // Note: Each cardinality has cardinality - 1 break points if c is cardinality
    //       the breakpoints can be found in the following array positions: 
    //       FROM (c - 1) * (c - 2) / 2 
    //       TO   (c - 1) * (c - 2) / 2 + c - 1

    //printf("FROM %lf TO %lf\n", sax_breakpoints[offset], sax_breakpoints[offset + cardinality - 2]);

    __m256 vmask;
    __m256 vt;
    __m128 vmask1;

    for(int i=0;i<Segments;i++){

      vt = _mm256_set1_ps(paa[i]);
      vmask = _mm256_cmp_ps(vt, BM.a1, _CMP_GT_OQ);
      int bit_mask = _mm256_movemask_ps(vmask);

      int d = sax_c1[bit_mask];
      vmask = _mm256_cmp_ps(vt, BM.a2[d], _CMP_GT_OQ);
      bit_mask = _mm256_movemask_ps(vmask);

      int d1 = sax_c1[bit_mask];
      vmask1 = _mm_cmp_ps(*(__m128*)&vt, BM.a3[d][d1], _CMP_GT_OQ);
      bit_mask = _mm_movemask_ps(vmask1);

      sax_out[i] = (d << 5) + (d1 << 2) + sax_c1[bit_mask];
    }
    
    //sax_print(sax_out, segments, cardinality);

    
}

void saxt_from_ts(ts_type *ts_in, saxt_type *saxt_out) {
    // Create PAA representation
    float paa[Segments];
    sax_type sax_out[Segments];

    __m256 xfsSum;
    __m256 a;
    for (int i=0; i<Segments; i++) {
      xfsSum = _mm256_setzero_ps();
      int off = i * Ts_values_per_segment;
      for (int j=0;j<Ts_values_per_segment;j+=8) {
        a = _mm256_loadu_ps(ts_in + off + j);
        xfsSum = _mm256_add_ps(xfsSum, a);
      }
      const auto* q = (const float*)&xfsSum;
      paa[i] = (q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7]) / Ts_values_per_segment;
    }

    // Convert PAA to SAX
    // Note: Each cardinality has cardinality - 1 break points if c is cardinality
    //       the breakpoints can be found in the following array positions:
    //       FROM (c - 1) * (c - 2) / 2
    //       TO   (c - 1) * (c - 2) / 2 + c - 1
    //printf("FROM %lf TO %lf\n", sax_breakpoints[offset], sax_breakpoints[offset + cardinality - 2]);

    __m256 vmask;
    __m256 vt;
    __m128 vmask1;

    for(int i=0;i<Segments;i++){

      vt = _mm256_set1_ps(paa[i]);
      vmask = _mm256_cmp_ps(vt, BM.a1, _CMP_GT_OQ);
      int bit_mask = _mm256_movemask_ps(vmask);

      int d = sax_c1[bit_mask];
      vmask = _mm256_cmp_ps(vt, BM.a2[d], _CMP_GT_OQ);
      bit_mask = _mm256_movemask_ps(vmask);

      int d1 = sax_c1[bit_mask];
      vmask1 = _mm_cmp_ps(*(__m128*)&vt, BM.a3[d][d1], _CMP_GT_OQ);
      bit_mask = _mm_movemask_ps(vmask1);

      sax_out[i] = (d << 5) + (d1 << 2) + sax_c1[bit_mask];
    }

//    uint64_t t = (*(uint64_t*)sax_out);
//    (*(uint64_t*)saxt_out) = ((t & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000 |
//                              (((((t) & 0x4040404040404040) * 0x2040810204081) & 0x7f80000000000000) >> 7) |
//                              (((((t) & 0x2020202020202020) * 0x2040810204081) & 0x3fc0000000000000) >> 14) |
//                              (((((t) & 0x1010101010101010) * 0x2040810204081) & 0x1fe0000000000000) >> 21) |
//                              (((((t) & 0x0808080808080808) * 0x2040810204081) & 0x0ff0000000000000) >> 28) |
//                              (((((t) & 0x0404040404040404) * 0x2040810204081) & 0x07f8000000000000) >> 35) |
//                              (((((t) & 0x0202020202020202) * 0x2040810204081) & 0x03fc000000000000) >> 42) |
//                              (((((t) & 0x0101010101010101) * 0x2040810204081) & 0x01fe000000000000) >> 49);

#if is8s
    uint64_t t = (*(uint64_t*)sax_out);
  (*(uint64_t*)saxt_out) = ((t & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000 |
                           (((((t<<1) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 8) |
                           (((((t<<2) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 16) |
                           (((((t<<3) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 24) |
                           (((((t<<4) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 32) |
                           (((((t<<5) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 40) |
                           (((((t<<6) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 48) |
                           (((((t<<7) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 56);
#else
  for(int i=0;i<Bit_cardinality;i++) {
    saxt_out[i] = _mm_movemask_epi8(_mm_slli_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i_u *>(sax_out)), Bit_cardinality - i - 1));
  }
#endif

    
}

void paa_saxt_from_ts(ts_type *ts_in, saxt_type *saxt_out, ts_type *paa) {
  // Create PAA representation
  sax_type sax_out[Segments];

  __m256 xfsSum;
  __m256 a;
  for (int i=0; i<Segments; i++) {
    xfsSum = _mm256_setzero_ps();
    int off = i * Ts_values_per_segment;
    for (int j=0;j<Ts_values_per_segment;j+=8) {
      a = _mm256_loadu_ps(ts_in + off + j);
      xfsSum = _mm256_add_ps(xfsSum, a);
    }
    const auto* q = (const float*)&xfsSum;
    paa[i] = (q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7]) / Ts_values_per_segment;
  }

  // Convert PAA to SAX
  // Note: Each cardinality has cardinality - 1 break points if c is cardinality
  //       the breakpoints can be found in the following array positions:
  //       FROM (c - 1) * (c - 2) / 2
  //       TO   (c - 1) * (c - 2) / 2 + c - 1
  //printf("FROM %lf TO %lf\n", sax_breakpoints[offset], sax_breakpoints[offset + cardinality - 2]);

  __m256 vmask;
  __m256 vt;
  __m128 vmask1;

  for(int i=0;i<Segments;i++){

    vt = _mm256_set1_ps(paa[i]);
    vmask = _mm256_cmp_ps(vt, BM.a1, _CMP_GT_OQ);
    int bit_mask = _mm256_movemask_ps(vmask);

    int d = sax_c1[bit_mask];
    vmask = _mm256_cmp_ps(vt, BM.a2[d], _CMP_GT_OQ);
    bit_mask = _mm256_movemask_ps(vmask);

    int d1 = sax_c1[bit_mask];
    vmask1 = _mm_cmp_ps(*(__m128*)&vt, BM.a3[d][d1], _CMP_GT_OQ);
    bit_mask = _mm_movemask_ps(vmask1);

    sax_out[i] = (d << 5) + (d1 << 2) + sax_c1[bit_mask];
  }

#if is8s
  uint64_t t = (*(uint64_t*)sax_out);
  (*(uint64_t*)saxt_out) = ((t & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000 |
                           (((((t<<1) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 8) |
                           (((((t<<2) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 16) |
                           (((((t<<3) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 24) |
                           (((((t<<4) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 32) |
                           (((((t<<5) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 40) |
                           (((((t<<6) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 48) |
                           (((((t<<7) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 56);
#else
  for(int i=0;i<Bit_cardinality;i++) {
    saxt_out[i] = _mm_movemask_epi8(_mm_slli_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i_u *>(sax_out)), Bit_cardinality - i - 1));
  }
#endif

  
}

void saxt_from_sax(sax_type *sax_in, saxt_type *saxt_out) {
#if is8s
  uint64_t t = (*(uint64_t*)sax_in);
  (*(uint64_t*)saxt_out) = ((t & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000 |
                           (((((t<<1) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 8) |
                           (((((t<<2) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 16) |
                           (((((t<<3) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 24) |
                           (((((t<<4) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 32) |
                           (((((t<<5) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 40) |
                           (((((t<<6) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 48) |
                           (((((t<<7) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 56);
#else
  for(int i=0;i<Bit_cardinality;i++) {
    saxt_out[i] = _mm_movemask_epi8(_mm_slli_epi64(_mm_loadu_si128(reinterpret_cast<const __m128i_u *>(sax_in)), Bit_cardinality - i - 1));
  }
#endif
    
}

void sax_from_saxt(saxt_type *saxt_in, sax_type *sax_out) {
//  uint64_t t = 0;
//  memcpy(&t, saxt_in, sizeof(uint64_t));
#if is8s
  uint64_t t = (*(uint64_t*)saxt_in);
  (*(uint64_t*)sax_out) = ((t & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000 |
                          (((((t<<1) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 8) |
                          (((((t<<2) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 16) |
                          (((((t<<3) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 24) |
                          (((((t<<4) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 32) |
                          (((((t<<5) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 40) |
                          (((((t<<6) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 48) |
                          (((((t<<7) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 56);
#else
  uint64_t t[2];
  memset(t, 0, sizeof t);
  memcpy(t, saxt_in, sizeof(saxt_only));
  for(int i=0;i<Segments;i++) {
    int daoi = Segments - i - 1;
    sax_out[i] = ((((t[0] << daoi) & 0x8000800080008000) * 0x200040008001) >> 60) |
             ((((t[1] << daoi) & 0x8000800080008000) * 0x200040008001) >> 56) ;
  }
#endif

    
}


void printbin(long long unsigned int n, int size) {
#if isprint
    char *b = static_cast<char *>(malloc(sizeof(char) * (size + 1)));
    int i;
    
    for (i=0; i<size; i++) {
        b[i] = '0';
    }
    
    for (i=0; i<size; i++, n=n/2)
        if (n%2) b[size-1-i] = '1';
    
    b[size] = '\0';
    printf("%s\n", b);
    free(b);
#endif
}

void serial_printbin (unsigned long long n, int size) {
    char *b = static_cast<char *>(malloc(sizeof(char) * (size + 1)));
    int i;
    
    for (i=0; i<size; i++) {
        b[i] = '0';
    }
    
    for (i=0; i<size; i++, n=n/2)
        if (n%2) b[size-1-i] = '1';
    
    b[size] = '\0';
    printf(" %s ", b);
}


/**
 This function prints a sax record.
 */
void sax_print(sax_type *sax, int segments, int bit_cardinality)
{
    int i;
    for (i=0; i < segments; i++) {
        printf("%d:\t", i);
        printbin(sax[i], bit_cardinality);
    }
    printf("\n");
}

void sax_print(sax_type *sax) {
  int i;
  for (i=0; i < Segments; i++) {
//        printf("%d:\t", i);
    std::cout<<(int)sax[i]<<" ";
//        printbin(saxt[i], Segments);
  }
  printf("\n");
}

void saxt_print(saxt_type *saxt) {
#if isprint
    int i;
    for (i=0; i < Bit_cardinality; i++) {
//        printf("%d:\t", i);
        std::cout<<(int)saxt[i]<<" ";
//        printbin(saxt[i], Segments);
    }
    printf("\n");
#endif
}

void saxt_print(saxt_only saxt) {
#if isprint
  int i;
  for (i=0; i < Bit_cardinality; i++) {
//        printf("%d:\t", i);
    std::cout<<(int)saxt.asaxt[i]<<" ";
//        printbin(saxt[i], Segments);
  }
  printf("\n");
#endif
}

void leafkey_print(void* saxt) {
  int i;
  for (i=0; i < Bit_cardinality*2; i++) {
//        printf("%d:\t", i);
    std::cout<<(int)((unsigned char*)saxt)[i]<<" ";
//        printbin(saxt[i], Segments);
  }
  printf("\n");
}

void saxt_print(saxt_type *saxt, saxt_type *prefix, cod co_d) {
#if isprint
    int i;
    for (i=0; i < co_d; i++) {
//        printf("%d:\t", i);
//        printbin(prefix[i], Segments);
      std::cout<<(int)prefix[i]<<" ";
    }
    for (i=co_d; i < Bit_cardinality; i++) {
//        printf("%d:\t", i);
//        printbin(saxt[i-co_d], Segments);
      std::cout<<(int)saxt[i-co_d]<<" ";
    }
    printf("\n");
#endif
}

float minidist_paa_to_saxt(const float* paa, saxt saxt_, cod co_d) {

    ts_type distance = 0;
    // TODO: Store offset in index settings. and pass index settings as parameter.

    int offset = sax_offset_i[co_d];
    sax_type sax[Segments];
    // For each sax record find the break point

#if is8s
    uint64_t t = 0;
    memcpy(&t, saxt_, sizeof(saxt_type) * co_d);
    (*(uint64_t*)sax) = ((t & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000 |
                        (((((t<<1) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 8) |
                        (((((t<<2) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 16) |
                        (((((t<<3) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 24) |
                        (((((t<<4) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 32) |
                        (((((t<<5) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 40) |
                        (((((t<<6) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 48) |
                        (((((t<<7) & 0x8080808080808080) * 0x2040810204081) & 0xff00000000000000) >> 56);
#else
  uint64_t t[2];
  memset(t, 0, sizeof t);
  memcpy(t, saxt_, sizeof(saxt_type) * co_d);
  for(int i=0;i<Segments;i++) {
    int daoi = Segments - i - 1;
    sax[i] = ((((t[0] << daoi) & 0x8000800080008000) * 0x200040008001) >> 60) |
             ((((t[1] << daoi) & 0x8000800080008000) * 0x200040008001) >> 56) ;
  }
#endif

    ts_type breakpoint_lower[Segments];
    memset(breakpoint_lower, 0, sizeof breakpoint_lower);
    ts_type breakpoint_upper[Segments];
    memset(breakpoint_upper, 0, sizeof breakpoint_upper);

    for (int i=0; i<Segments; i++) {
//        for(int j=co_d-1; j>=0; j--) {
//          sax[i] |= (saxt_[j]>>(i) & 1) << (j);
//        }
        sax_type region = sax[i];

        /*
            int region_lower = v << (c_m - c_c);
            int region_upper = (~((int)MAXFLOAT << (c_m - c_c)) | region_lower);
        */

        breakpoint_lower[i] = region == 0 ? -MAXFLOAT : sax_breakpoints[offset + region - 1];
        breakpoint_upper[i] = region == cardinality_1_i[co_d] ? MAXFLOAT : sax_breakpoints[offset + region];

        //printf("FROM: \n");
        //sax_print(&region_lower, 1, c_m);
        //printf("TO: \n");
        //sax_print(&region_upper, 1, c_m);
        //printf("\n%d.%d is from %d to %d, %lf - %lf\n", v, c_c, region_lower, region_upper,
        //       breakpoint_lower, breakpoint_upper);
//        if (breakpoint_lower[i] > paa[i]) {
//          distance += pow(breakpoint_lower[i] - paa[i], 2);
//        }
//        else if(breakpoint_upper[i] < paa[i]) {
//          distance += pow(breakpoint_upper[i] - paa[i], 2);
//        }

//        else {
//            printf("%lf is between: %lf and %lf\n", paa[i], breakpoint_lower, breakpoint_upper);
//        }
    }
//
//    float distance1 = 0;
#if is8s
  __m256 dis8 = _mm256_setzero_ps();
  __m256 paa8 = _mm256_loadu_ps(paa);

  __m256 lower8 = _mm256_loadu_ps(breakpoint_lower);
  __m256 tosub = _mm256_sub_ps(lower8, paa8);
  __m256 f = _mm256_mul_ps(tosub, tosub);
  __m256 mask = _mm256_cmp_ps(lower8, paa8, _CMP_GT_OQ);
  dis8 = _mm256_add_ps(dis8, _mm256_and_ps(f, mask));

  __m256 upper8 = _mm256_loadu_ps(breakpoint_upper);
  tosub = _mm256_sub_ps(upper8, paa8);
  f = _mm256_mul_ps(tosub, tosub);
  mask = _mm256_cmp_ps(upper8, paa8, _CMP_LT_OQ);
  dis8 = _mm256_add_ps(dis8, _mm256_and_ps(f, mask));

  const auto* q = (const float*)&dis8;
  distance = q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7];

#else
  __m256 dis8 = _mm256_setzero_ps();

  for(int i=0;i<Segments;i+=8) {
    __m256 paa8 = _mm256_loadu_ps(paa + i);

    __m256 lower8 = _mm256_loadu_ps(breakpoint_lower + i);
    __m256 tosub = _mm256_sub_ps(lower8, paa8);
    __m256 f = _mm256_mul_ps(tosub, tosub);
    __m256 mask = _mm256_cmp_ps(lower8, paa8, _CMP_GT_OQ);
    dis8 = _mm256_add_ps(dis8, _mm256_and_ps(f, mask));

    __m256 upper8 = _mm256_loadu_ps(breakpoint_upper + i);
    tosub = _mm256_sub_ps(upper8, paa8);
    f = _mm256_mul_ps(tosub, tosub);
    mask = _mm256_cmp_ps(upper8, paa8, _CMP_LT_OQ);
    dis8 = _mm256_add_ps(dis8, _mm256_and_ps(f, mask));

  }

  const auto* q = (const float*)&dis8;
  distance = q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7];
#endif

//  if (abs(distance - distance1) > 1e6) {
//    out("错了");
//    out(distance);
//    out(distance1);
//    ts_print((float*)paa, 16);
//    sax_print(sax);
//    exit(1);
//  }

    //distance = ratio_sqrt * sqrtf(distance);
    return nchuw * distance;
}

ts_type minidist_paa_to_isax(ts_type  *paa, sax_type *sax,
                             sax_type *sax_cardinalities,
                             sax_type max_bit_cardinality,
                             sax_type max_cardinality,
                             int number_of_segments,
                             int min_val,
                             int max_val,
                             float ratio_sqrt)
{

    ts_type distance = 0;
    // TODO: Store offset in index settings. and pass index settings as parameter.

    int offset = ((max_cardinality - 1) * (max_cardinality - 2)) / 2;
    
    // For each sax record find the break point
    int i;
    for (i=0; i<number_of_segments; i++) {
        
        sax_type c_c = sax_cardinalities[i];
        sax_type c_m = max_bit_cardinality;
        sax_type v = sax[i];
        
        
        sax_type region_lower = v << (c_m - c_c);
        sax_type region_upper = (~((int)MAXFLOAT << (c_m - c_c)) | region_lower);
        

	/*
        int region_lower = v << (c_m - c_c);
        int region_upper = (~((int)MAXFLOAT << (c_m - c_c)) | region_lower);
	*/
	
        ts_type breakpoint_lower = 0; // <-- TODO: calculate breakpoints.
        ts_type breakpoint_upper = 0; // <-- - || -
        
        
        if (region_lower == 0) {
            breakpoint_lower = min_val;
        }
        else
        {
            breakpoint_lower = sax_breakpoints[offset + region_lower - 1];
        }
        if (region_upper == max_cardinality - 1) {
            breakpoint_upper = max_val;
        }
        else
        {
            breakpoint_upper = sax_breakpoints[offset + region_upper];
        }
        //printf("FROM: \n");
        //sax_print(&region_lower, 1, c_m);
        //printf("TO: \n");
        //sax_print(&region_upper, 1, c_m);
        //printf("\n%d.%d is from %d to %d, %lf - %lf\n", v, c_c, region_lower, region_upper,
        //       breakpoint_lower, breakpoint_upper);
        
        if (breakpoint_lower > paa[i]) {
            distance += pow(breakpoint_lower - paa[i], 2);
        }
        else if(breakpoint_upper < paa[i]) {
            distance += pow(breakpoint_upper - paa[i], 2);
        }
//        else {
//            printf("%lf is between: %lf and %lf\n", paa[i], breakpoint_lower, breakpoint_upper);
//        }
    }
    
    //distance = ratio_sqrt * sqrtf(distance);  
    distance = ratio_sqrt * distance;
    return distance;
}


ts_type minidist_paa_to_isax_raw(ts_type *paa, sax_type *sax, 
                           sax_type *sax_cardinalities,
                           sax_type max_bit_cardinality,
                           int max_cardinality,
                           int number_of_segments,
                           int min_val,
                           int max_val,
                           ts_type ratio_sqrt) 
{
   
    float distance = 0;
    // TODO: Store offset in index settings. and pass index settings as parameter.
    
    int offset = ((max_cardinality - 1) * (max_cardinality - 2)) / 2;
    
    // For each sax record find the break point
    int i;
    for (i=0; i<number_of_segments; i++) {
        
        sax_type c_c = sax_cardinalities[i];
        sax_type c_m = max_bit_cardinality;
        sax_type v = sax[i];
        //sax_print(&v, 1, c_m);
        
        sax_type region_lower = (v >> (c_m - c_c)) <<  (c_m - c_c);
        sax_type region_upper = (~((int)MAXFLOAT << (c_m - c_c)) | region_lower);
		//printf("[%d, %d] %d -- %d\n", sax[i], c_c, region_lower, region_upper);

        float breakpoint_lower = 0; // <-- TODO: calculate breakpoints.
        float breakpoint_upper = 0; // <-- - || -
        
        
        if (region_lower == 0) {
            breakpoint_lower = min_val;
        }
        else
        {
            breakpoint_lower = sax_breakpoints[offset + region_lower - 1];
        }
        if (region_upper == max_cardinality - 1) {
            breakpoint_upper = max_val;
        }
        else
        {
            breakpoint_upper = sax_breakpoints[offset + region_upper];
        }

        //printf("\n%d.%d is from %d to %d, %lf - %lf\n", v, c_c, region_lower, region_upper,
        //       breakpoint_lower, breakpoint_upper);

        //printf("FROM: \n");
        //sax_print(&region_lower, 1, c_m);
        //printf("TO: \n");
        //sax_print(&region_upper, 1, c_m);
		
		//printf ("\n---------\n");
        
        if (breakpoint_lower > paa[i]) {
            distance += pow(breakpoint_lower - paa[i], 2);
        }
        else if(breakpoint_upper < paa[i]) {
            distance += pow(breakpoint_upper - paa[i], 2);
        }
//        else {
//            printf("%lf is between: %lf and %lf\n", paa[i], breakpoint_lower, breakpoint_upper);
//        }
    }
    
    //distance = ratio_sqrt * sqrtf(distance);
    distance = ratio_sqrt * distance;
    return distance;
}













