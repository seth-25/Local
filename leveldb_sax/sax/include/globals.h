//
//  defines.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/19/12.
//  Copyright 2012 University of Trento. All rights reserved.
//



#include "bitset"
#include "config.h"
#include "iostream"
#include <cassert>
#include "cstring"
#include "sax_bsearch.h"
#include "immintrin.h"
#ifndef isax_globals_h
#define isax_globals_h


#define daxiao 1 // 0为16字节， 1为8字节
// 0, 1, 2 代表 不要时间， 要时间但不存，要时间存
#define istime 0
// 是否统计精确查询所计算下界距离的saxt 0不统计 1统计
#define iscount_saxt_for_exact 0
// 是否使用贪心策略 0不使用 1使用
#define isgreed 1
// 是否print
#define isprint 0
// 0, 1, 2 代表 一个，一部分，一个叶
#define lookupi 2
// 初始化时存入st
#define init_st 1
//近似查询是否排序后一起查，1不，0要
#define cha 0
//是否一个一个传
#define isap 1

#define Get_div 20

///// TYPES /////
#if isprint
#define out(a) std::cout<<a<<std::endl
#define out1(a,b) std::cout<<a<<" "<<to_string(b)<<std::endl
#else
#define out(a) //std::cout<<a<<std::endl
#define out1(a,b) //std::cout<<a<<" "<<to_string(b)<<std::endl
#endif
//这里基数256变为512则用short
//typedef unsigned short sax_type;
typedef unsigned char sax_type;
typedef sax_type* sax;

//段数为16，为8变为char
#if daxiao
typedef unsigned char saxt_type;
#else
typedef unsigned short saxt_type;
#endif
typedef saxt_type* saxt;
typedef saxt_type* saxt_prefix;
typedef float ts_type;
typedef ts_type *ts;
typedef time_t ts_time;

typedef unsigned char cod;

#define Cardinality 256
#define Bit_cardinality 8
#if daxiao
#define Segments 8
#define nchuw 32 // Ts_length / Segments
#define Ts_values_per_segment 32
#else
#define Segments 16
#define nchuw 16 // Ts_length / Segments
#define Ts_values_per_segment 16
#endif


#define Ts_length 256
#define Leaf_maxnum 2048
#define Leaf_minnum Leaf_maxnum/2
//最小
#define Leaf_maxnum_rebalance 10
#define Leaf_minnum_rebalance 5

//初始化的数量==内存表中存的数量
#define init_num 1000000

//一个memtable存的数量
#define Table_maxnum 500000

// 压缩im的线程数，一般与表数量一致
#define pool_size init_num/Table_maxnum

//精确查询一个表需要多线程的
#define get_exact_multiThread_file_size 500*1024*1024

#define pool_get_size 16


//超过这个重构叶结点
static const int Leaf_rebuildnum = Leaf_maxnum * 2;

//static int cardinality = 256;
//static int bit_cardinality = 8;
//static int segments = 8;

static const int sax_offset = ((Cardinality - 1) * (Cardinality - 2)) / 2;
static int sax_offset_i[Bit_cardinality+1] = {0,0,3,21,105,465,1953,8001,32385};
static int cardinality_1_i[Bit_cardinality+1] = {0,1,3,7,15,31,63,127,255};

typedef struct {
  ts_type ts[Ts_length];
} ts_only;

typedef struct saxt_only_rep{
  saxt_type asaxt[Bit_cardinality];

  saxt_only_rep() {}

  saxt_only_rep(const void* saxt_) {
    memcpy(asaxt, saxt_, sizeof(saxt_only_rep));
  }

  bool operator< (const saxt_only_rep& a) const {
#if daxiao
    return *(uint64_t*)asaxt < *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt < *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) < *(((uint64_t*)a.asaxt)+1);
#endif
  }
  bool operator> (const saxt_only_rep& a) const {
#if daxiao
    return *(uint64_t*)asaxt > *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt > *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) > *(((uint64_t*)a.asaxt)+1);
#endif
  }

  bool operator<= (const saxt_only_rep& a) const {
#if daxiao
    return *(uint64_t*)asaxt <= *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt <= *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) < *(((uint64_t*)a.asaxt)+1);
#endif
  }
  bool operator>= (const saxt_only_rep& a) const {
#if daxiao
    return *(uint64_t*)asaxt >= *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt >= *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) > *(((uint64_t*)a.asaxt)+1);
#endif
  }

  bool operator== (const saxt_only_rep& a) const {
#if daxiao
    return *(uint64_t*)asaxt == *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) && *(uint64_t*)asaxt == *(uint64_t*)a.asaxt;
#endif
  }

} saxt_only;

typedef struct {
  ts_type apaa[Segments];
} paa_only;

typedef struct {
  ts_type ts[Ts_length];
#if istime
  ts_time tsTime;
#endif
} tsKey;



typedef struct {
  ts_type ts[Ts_length];
#if istime
  ts_time startTime;
  ts_time endTime;
#endif
} aquery_rep;

typedef struct {
  aquery_rep rep;
  int k;
  ts_type paa[Segments];
  saxt_only asaxt;
} aquery;

typedef struct ares_exact_rep{
  tsKey atskey;
  float dist;

  bool operator< (const ares_exact_rep& a) const {
    return dist < a.dist;
  }
  bool operator> (const ares_exact_rep& a) const {
    return dist > a.dist;
  }
} ares_exact;

typedef struct ares{
  ares_exact rep;
  void* p;

  bool operator< (const ares& a) const {
    return rep < a.rep;
  }
  bool operator> (const ares& a) const {
    return rep > a.rep;
  }
} ares;



typedef std::pair<float, void*> dist_p;

static const size_t send_size1 = 1+sizeof(int)*2+sizeof(uint64_t)+sizeof(saxt_only)*2+sizeof(ts_time)*2;
static const size_t send_size2 = 1+sizeof(int)*3;
static const size_t send_size2_add = sizeof(uint64_t) + sizeof(saxt_only)*2 + sizeof(ts_time)*2;

static const size_t sizeinfo_pos = sizeof(aquery_rep) + sizeof(int)*2 + sizeof(float);

static const size_t to_find_size_leafkey = sizeof(aquery_rep) + sizeof(int)*3 + sizeof(float) + sizeof(void*);

static inline int compare_saxt(const void* a, const void* b) {
  if (*(saxt_only*)a < *(saxt_only*)b) return -1;
  if (*(saxt_only*)a > *(saxt_only*)b) return 1;
  return 0;
}

typedef struct to_bsear_rep {

  to_bsear_rep() {
    a1 = _mm256_loadu_ps(sax_a1);
    for(int i=0;i<8;i++) a2[i] = _mm256_loadu_ps(sax_a2[i]);
    for(int i=0;i<8;i++)
      for(int j=0;j<8;j++)
        a3[i][j] = _mm_loadu_ps(sax_a3[i][j]);
  }
  __m256 a1;
  __m256 a2[8];
  __m128 a3[8][8];
} to_bsear;

static to_bsear BM;



typedef unsigned long long file_position_type;
typedef unsigned long long root_mask_type;

enum response {OUT_OF_MEMORY_FAILURE, FAILURE, SUCCESS};
enum insertion_mode {PARTIAL = 1,
                     TMP = 2,
                     FULL = 4,
                     NO_TMP = 8};

enum buffer_cleaning_mode {FULL_CLEAN, TMP_ONLY_CLEAN, TMP_AND_TS_CLEAN};
enum node_cleaning_mode {DO_NOT_INCLUDE_CHILDREN = 0,
                         INCLUDE_CHILDREN = 1};

//
/////// DEFINITIONS /////
//#define MINVAL -2000000
//#define MAXVAL 2000000
//#define DELIMITER ' '
//#define TRUE 1
//#define FALSE 0
//#define BUFFER_REALLOCATION_RATE  2
//
/////// GLOBAL VARIABLES /////
//int FLUSHES;
//
/////// MACROS /////
//#define CREATE_MASK(mask, index, sax_array)\
//	int mask__i; \
//	for (mask__i=0; mask__i < index->settings->paa_segments; mask__i++) \
//		if(index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & sax_array[mask__i]) \
//			mask |= index->settings->bit_masks[index->settings->paa_segments - mask__i - 1];
//
/////// BNECHMARKING /////
////#ifdef BENCHMARK
//		#include <time.h>
//		#include <sys/time.h>
//
//        double tS;
//        double tE;
//
//        struct timeval total_time_start;
//        struct timeval parse_time_start;
//        struct timeval input_time_start;
//        struct timeval output_time_start;
//        struct timeval load_node_start;
//        struct timeval current_time;
//        struct timeval fetch_start;
//        struct timeval fetch_check_start;
//        double total_input_time;
//        double load_node_time;
//        double total_output_time;
//        double total_parse_time;
//        double total_time;
//
///*
//        int total_tree_nodes;
//        int loaded_nodes;
//        int checked_nodes;
//        file_position_type loaded_records;
//*/
//
//
//        struct timeval partial_time_start;
//        struct timeval partial_input_time_start;
//        struct timeval partial_output_time_start;
//        struct timeval partial_load_node_time_start;
//
//        double partial_time;
//        double partial_input_time;
//        double partial_output_time;
//        double partial_load_node_time;
//
//        unsigned long long partial_seq_input_count;
//        unsigned long long partial_seq_output_count;
//        unsigned long long partial_rand_input_count;
//        unsigned long long partial_rand_output_count;
//
//        unsigned long total_nodes_count;
//        unsigned long leaf_nodes_count;
//        unsigned long empty_leaf_nodes_count;
//        unsigned long loaded_nodes_count;
//        unsigned long checked_nodes_count;
//        unsigned long loaded_ts_count;
//        unsigned long checked_ts_count;
//        unsigned long total_ts_count;
//        unsigned long total_queries_count;
//        ts_type total_node_tlb;
//        ts_type total_data_tlb;
//
//        #define INIT_STATS() total_input_time = 0;\
//                             total_output_time = 0;\
//                             total_time = 0;\
//                             total_parse_time = 0;\
//                             load_node_time= 0;\
//			     partial_time = 0;\
//			     partial_input_time = 0;\
//			     partial_output_time = 0;\
//			     partial_load_node_time = 0;\
//                             partial_seq_input_count = 0;\
//                             partial_seq_output_count = 0;\
//                             partial_rand_input_count = 0;\
//                             partial_rand_output_count = 0;\
//			     total_nodes_count = 0;\
//			     leaf_nodes_count = 0;\
//			     empty_leaf_nodes_count = 0;\
//			     loaded_nodes_count = 0;\
//			     loaded_ts_count = 0;\
//			     checked_ts_count = 0;\
//                             checked_nodes_count = 0;\
//			     total_ts_count = 0;
//
//
///*
//        #define INIT_STATS() total_input_time = 0;\
//                             total_output_time = 0;\
//                             total_time = 0;\
//                             total_parse_time = 0;\
//                             total_tree_nodes = 0;\
//                             loaded_nodes = 0;\
//                             checked_nodes = 0;\
//                             load_node_time=0;\
//                             loaded_records = 0; \
//        printf("input\t output\t parse\t nodes\t checked_nodes\t loaded_nodes\t loaded_records\t distance\t load_node_time\t total\n");
//        #define PRINT_STATS(result_distance) printf("%lf\t %lf\t %lf\t %d\t %d\t %d\t %lld\t %lf\t %lf\t %lf\n", \
//        total_input_time, total_output_time, \
//        total_parse_time, total_tree_nodes, \
//        checked_nodes, loaded_nodes, \
//        loaded_records, result_distance, load_node_time, total_time);
//
//        #define COUNT_NEW_NODE() total_tree_nodes++;
//        #define COUNT_LOADED_NODE() loaded_nodes++;
//        #define COUNT_CHECKED_NODE() checked_nodes++;
//
//        #define COUNT_LOADED_RECORD() loaded_records++;
//
//        #define COUNT_INPUT_TIME_START gettimeofday(&input_time_start, NULL);
//        #define COUNT_OUTPUT_TIME_START gettimeofday(&output_time_start, NULL);
//        #define COUNT_TOTAL_TIME_START gettimeofday(&total_time_start, NULL);
//        #define COUNT_PARSE_TIME_START gettimeofday(&parse_time_start, NULL);
//        #define COUNT_LOAD_NODE_START gettimeofday(&load_node_start, NULL);
//        #define COUNT_INPUT_TIME_END  gettimeofday(&current_time, NULL); \
//                                      tS = input_time_start.tv_sec*1000000 + (input_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
//                                      total_input_time += (tE - tS);
//        #define COUNT_OUTPUT_TIME_END gettimeofday(&current_time, NULL); \
//                                      tS = output_time_start.tv_sec*1000000 + (output_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      total_output_time += (tE - tS);
//        #define COUNT_TOTAL_TIME_END  gettimeofday(&current_time, NULL); \
//                                      tS = total_time_start.tv_sec*1000000 + (total_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      total_time += (tE - tS);
//        #define COUNT_PARSE_TIME_END  gettimeofday(&current_time, NULL);  \
//                                      tS = parse_time_start.tv_sec*1000000 + (parse_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      total_parse_time += (tE - tS);
//        #define COUNT_LOAD_NODE_END   gettimeofday(&current_time, NULL);  \
//                                      tS = load_node_start.tv_sec*1000000 + (load_node_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      load_node_time += (tE - tS);
//
//*/
//
//
//        #define COUNT_NEW_NODE ++total_nodes_count;
//        #define COUNT_LEAF_NODE ++leaf_nodes_count;
//        #define COUNT_EMPTY_LEAF_NODE ++empty_leaf_nodes_count;
//        #define COUNT_EMPTY_LEAF_NODE_CANCEL --empty_leaf_nodes_count;
//        #define COUNT_TOTAL_TS(num_ts) total_ts_count+=num_ts; //actual ts inserted in index
//
//        #define COUNT_CHECKED_NODE ++checked_nodes_count;
//        #define COUNT_LOADED_NODE ++loaded_nodes_count;
//        #define COUNT_LOADED_TS(num_ts) loaded_ts_count +=num_ts; //ts loaded to answer query
//        #define COUNT_CHECKED_TS(num_ts) checked_ts_count +=num_ts; //ts loaded to answer query
//
//
//      #define RESET_QUERY_COUNTERS() loaded_nodes_count = 0;\
//                                     loaded_ts_count = 0;\
//                                     checked_nodes_count = 0;\
//                                     checked_ts_count = 0;
//
//      #define RESET_PARTIAL_COUNTERS() partial_seq_output_count = 0;\
//                                       partial_seq_input_count = 0;\
//                                       partial_rand_output_count = 0;\
//                                       partial_rand_input_count = 0;\
//				       partial_input_time = 0;\
//				       partial_output_time = 0;\
//				       partial_load_node_time = 0;\
//				       partial_time = 0;
//
//       #define PRINT_QUERY_COUNTERS() printf("loaded_nodes and checked_ts = %lu and %lu\n", loaded_nodes_count, checked_ts_count);
//       #define PRINT_PARTIAL_COUNTERS() printf("seq_output and partial_time = %llu and %f\n",partial_seq_output_count,  partial_time);
//
//        #define COUNT_PARTIAL_SEQ_INPUT ++partial_seq_input_count;
//        #define COUNT_PARTIAL_SEQ_OUTPUT ++partial_seq_output_count;
//        #define COUNT_PARTIAL_RAND_INPUT ++partial_rand_input_count;
//        #define COUNT_PARTIAL_RAND_OUTPUT ++partial_rand_output_count;
//
//
//        #define COUNT_INPUT_TIME_START gettimeofday(&input_time_start, NULL);
//        #define COUNT_OUTPUT_TIME_START gettimeofday(&output_time_start, NULL);
//        #define COUNT_TOTAL_TIME_START gettimeofday(&total_time_start, NULL);
//
//        #define COUNT_PARTIAL_TIME_START gettimeofday(&partial_time_start, NULL);
//        #define COUNT_PARTIAL_INPUT_TIME_START gettimeofday(&partial_input_time_start, NULL);
//        #define COUNT_PARTIAL_OUTPUT_TIME_START gettimeofday(&partial_output_time_start, NULL);
//        #define COUNT_PARTIAL_LOAD_NODE_TIME_START gettimeofday(&partial_load_node_time_start, NULL);
//
//        #define COUNT_PARSE_TIME_START gettimeofday(&parse_time_start, NULL);
//        #define COUNT_LOAD_NODE_START gettimeofday(&load_node_start, NULL);
//        #define COUNT_INPUT_TIME_END  gettimeofday(&current_time, NULL);\
//                                      tS = input_time_start.tv_sec*1000000 + (input_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
//                                      total_input_time += (tE - tS);
//        #define COUNT_OUTPUT_TIME_END gettimeofday(&current_time, NULL); \
//                                      tS = output_time_start.tv_sec*1000000 + (output_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      total_output_time += (tE - tS);
//        #define COUNT_TOTAL_TIME_END  gettimeofday(&current_time, NULL); \
//                                      tS = total_time_start.tv_sec*1000000 + (total_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      total_time += (tE - tS);
//        #define COUNT_PARTIAL_INPUT_TIME_END  gettimeofday(&current_time, NULL); \
//                                      tS = partial_input_time_start.tv_sec*1000000 + (partial_input_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
//                                      partial_input_time += (tE - tS);
//        #define COUNT_PARTIAL_LOAD_NODE_TIME_END  gettimeofday(&current_time, NULL); \
//                                      tS = partial_load_node_time_start.tv_sec*1000000 + (partial_load_node_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000 + (current_time.tv_usec); \
//                                      partial_load_node_time += (tE - tS);
//        #define COUNT_PARTIAL_OUTPUT_TIME_END gettimeofday(&current_time, NULL); \
//                                      tS = partial_output_time_start.tv_sec*1000000 + (partial_output_time_start.tv_usec); \
//				      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      partial_output_time += (tE - tS);
//        #define COUNT_PARTIAL_TIME_END  gettimeofday(&current_time, NULL); \
//                                      tS = partial_time_start.tv_sec*1000000 + (partial_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      partial_time += (tE - tS);
//        #define COUNT_PARSE_TIME_END  gettimeofday(&current_time, NULL);  \
//                                      tS = parse_time_start.tv_sec*1000000 + (parse_time_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      total_parse_time += (tE - tS);
//        #define COUNT_LOAD_NODE_END   gettimeofday(&current_time, NULL);  \
//                                      tS = load_node_start.tv_sec*1000000 + (load_node_start.tv_usec); \
//                                      tE = current_time.tv_sec*1000000  + (current_time.tv_usec); \
//                                      load_node_time += (tE - tS);
//
///*
//  #else
//        #define INIT_STATS() ;
//        #define PRINT_STATS() ;
//        #define COUNT_NEW_NODE() ;
//        #define COUNT_CHECKED_NODE();
//        #define COUNT_LOADED_NODE() ;
//        #define COUNT_LOADED_RECORD() ;
//        #define COUNT_INPUT_TIME_START ;
//        #define COUNT_INPUT_TIME_END ;
//        #define COUNT_OUTPUT_TIME_START ;
//        #define COUNT_OUTPUT_TIME_END ;
//        #define COUNT_TOTAL_TIME_START ;
//        #define COUNT_TOTAL_TIME_END ;
//        #define COUNT_PARSE_TIME_START ;
//        #define COUNT_PARSE_TIME_END ;
//    #endif
//
//*/
#endif
