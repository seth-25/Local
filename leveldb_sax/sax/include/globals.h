#include "bitset"
#include "config.h"
#include "iostream"
#include <cassert>
#include "cstring"
#include "sax_bsearch.h"
#include "immintrin.h"
#include "atomic"
#ifndef isax_globals_h
#define isax_globals_h

// The number of segments a time series is divided into, support 8 and 16.
// 0 is 16, 1 is 8
#define is8_segment 0

#define Cardinality 256
#define Bit_cardinality 8
#if is8_segment
#define Segments 8
#else
#define Segments 16
#endif


// Whether the data contains a timestamp, 0:No timestamp, 1:has timestamp but not store, 2: has timestamp and store
#define istime 0


// The size of time series (float)
#define Ts_length 256
#define nchuw (Ts_length / Segments)
#define Ts_values_per_segment (Ts_length / Segments)

// Maximum number of keys stored in a node. NN in the paper.
#define Leaf_maxnum 512
//#define Leaf_maxnum (int)(128* 0.82)

// Minimum number of keys stored in a node. NM in the paper.
#define Leaf_minnum (Leaf_maxnum/2)

// Number of memtables (Number of insert threads)
#define pool_size 1

// Number of time series inserted in the initialization process
// init_num = pool_size * memtable_size
#define init_num (1 * (int)1e6)

// Size of memtable
#define memtable_size (init_num/pool_size)

// Number of compaction threads
#define pool_compaction_size 2

// Number of query threads
#define pool_get_size 24

// Divide the ones batch of SAXTs which less than topdis into several parts and query the raw time series for each part.
//#define Get_div 2
#define Get_div1 5 // approximate query
#define Get_div 20 // exact query

// The maximum number of Pos stored in the queue for accessing time series.
// When the number of time series accessed during each exact query is high, it is necessary to increase the value.
//#define info_p_max_size 250000000
//#define info_p_max_size 50000000
#define info_p_max_size 10240

// When the number of time series accessed during each exact query is high, it is necessary to set sort_p=1, to enable sequential disk access.
// 1 to sort by pos, 0 to sort by ldb.
// GAL(step 1)
#define sort_p 0


// Whether to record the time of compaction.
#define is_compaction_time 1

// Whether to count the number of SAXTs involved in computing the lower bound distance. 0 Not count, 1 count.
#define iscount_saxt_for_exact 1

// Whether to print debug information
#define isprint 0









/////////////// The following parameters do not need to be modified /////////////////
#define shunxu 0  // 1为不进行筛选，测试顺序时用，需令sort_p = 1
// 0, 1, 2 代表 一个，一部分，一个叶
#define lookupi 2
// 初始化时存入st
#define init_st 1
//近似查询是否排序后一起查，1不，0要
#define cha 0
//是否java直接调用堆，0不是，1是
#define isap 1
// 只有一个文件不考虑hash 0 不考虑 1考虑
#define ishash 0

#if isprint
#define out(a) std::cout<<a<<std::endl
#define out1(a,b) std::cout<<a<<" "<<to_string(b)<<std::endl
#define out2(a) std::cout<<a<<std::endl
#else
#define out(a) //std::cout<<a<<std::endl
#define out1(a,b) //std::cout<<a<<" "<<to_string(b)<<std::endl
#define out2(a) //std::cout<<a<<std::endl
#endif
//这里基数256变为512则用short
//typedef unsigned short sax_type;
typedef unsigned char sax_type;
typedef sax_type* sax;

//段数为16，为8变为char
#if is8_segment
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


#define isgreed 1 // 是否使用贪心策略 0不使用 1使用
#define ischaone 0 //1 只查一个节点，0 使用下面两种策略
#define ischalr 0 // 1必须查左右兄弟结点,0相距度一样时查兄弟节点
#define islevel0 1  // 1不合并不查，合并了才查，0都要查


//最小
#define Leaf_maxnum_rebalance 10


// 精确查询时，不同表大小不同，将大的表拆分给多个线程。拆分边界大小
#define get_exact_multiThread_file_size (1000*1024*1024)


// 压缩合并申请的缓存大小， 几个leaf
#define compaction_leaf_size 20
// 压缩合并 把前缀压缩放哪个线程里， 0 放主线程， 1 放snap压缩线程
#define qiehuan 0

#define input_buffer_size 2048  // 缓冲区






//超过这个重构叶结点
static const int Leaf_rebuildnum = Leaf_maxnum * 2;
static const int compaction_buffer_size = Leaf_rebuildnum;

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
#if is8_segment
    return *(uint64_t*)asaxt < *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt < *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) < *(((uint64_t*)a.asaxt)+1);
#endif
  }
  bool operator> (const saxt_only_rep& a) const {
#if is8_segment
    return *(uint64_t*)asaxt > *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt > *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) > *(((uint64_t*)a.asaxt)+1);
#endif
  }

  bool operator<= (const saxt_only_rep& a) const {
#if is8_segment
    return *(uint64_t*)asaxt <= *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt <= *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) < *(((uint64_t*)a.asaxt)+1);
#endif
  }
  bool operator>= (const saxt_only_rep& a) const {
#if is8_segment
    return *(uint64_t*)asaxt >= *(uint64_t*)a.asaxt;
#else
    return *(((uint64_t*)asaxt)+1) == *(((uint64_t*)a.asaxt)+1) ?
           *(uint64_t*)asaxt >= *(uint64_t*)a.asaxt : *(((uint64_t*)asaxt)+1) > *(((uint64_t*)a.asaxt)+1);
#endif
  }

  bool operator== (const saxt_only_rep& a) const {
#if is8_segment
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

#if isap
static const size_t to_find_size_leafkey = sizeof(aquery_rep) + sizeof(int)*3 + sizeof(float) + sizeof(void*);
#else
static const size_t to_find_size_leafkey = sizeof(aquery_rep) + sizeof(int)*3 + sizeof(float);
#endif


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

#endif
