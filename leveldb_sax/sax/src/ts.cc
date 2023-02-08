//
//  ts.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../include/ts.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xmmintrin.h>

#include "../include/config.h"
#include "../include/globals.h"
#include "immintrin.h"

/**
 This function converts a string of floats seperated by a delimeter into a ts 
 record of a size ts_size.
 @param char ts_str[]
 @param int ts_size
 @param const char * delims
 @return *ts
 */
void ts_parse_str(char ts_str[], ts_type * ts_out, int ts_size, const char * delims)
{
    int index=0;
    char *result = strtok( ts_str, delims );
	while( result != NULL ) {
		ts_out[index] = atof(result);
		result = strtok( NULL, delims );
#ifdef SANITY_CHECK
        if (index >= ts_size)
        {
            fprintf(stderr, "sanity error: Time series bigger than limit of %d", ts_size);
            exit(-1); 
        }
#endif
        index++;
	}
    free(result);
}

float ts_euclidean_distance(ts_type * t, ts_type * s, int size) {

  __m256 xfsSum = _mm256_setzero_ps();
  __m256 a;	// 加载.
  __m256 b;

  for (int i=0;i<size;i+=8) {
    a = _mm256_loadu_ps(t + i);
    b = _mm256_loadu_ps(s + i);
    __m256 c = _mm256_sub_ps(a, b);
    __m256 d = _mm256_mul_ps(c, c);
    xfsSum = _mm256_add_ps(xfsSum, d);
  }
  const auto* q = (const float*)&xfsSum;
  return q[0] + q[1] + q[2] + q[3] + q[4] + q[5] + q[6] + q[7];

//    float distance = 0;
//    while (size > 0) {
//        size--;
//        distance += (t[size] - s[size]) * (t[size] - s[size]);
//    }
//    //distance = sqrtf(distance);
//
//    return distance;
}

ts_type ts_euclidean_distance_reordered(ts_type * q, ts_type * t , int j , int  size ,ts_type bsf, int * order)
{
    int i;
    ts_type sum = 0;
    for ( i = 0 ; i < size && sum < bsf ; i++ )
    {
       //ts_type x = (T[(order[i]+j)]-mean)/std;
      ts_type x = t[order[i]];
      sum += (x-q[i])*(x-q[i]);      
    }
    return sum;
}




/** 
 This function prints a ts record of a size.
 @param ts *ts
 @param int size
*/
void ts_print(ts_type *ts, int size) 
{
    int i;
    for (i=0; i < size; i++) {
        printf("%lf ", ts[i]);
    }
    printf("\n");
}
