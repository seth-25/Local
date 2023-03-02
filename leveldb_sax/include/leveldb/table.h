// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>
#include <db/query_heap.h>
#include <jni/send_master.h>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

#include "STLeaf.h"
#include "STNonLeaf.h"
#include "stack"
#include "zsbtree/zsbtree_table.h"
#include "threadPool_2.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;


// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class LEVELDB_EXPORT Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Table** table);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;



 private:
  friend class TableCache;
  struct Rep;





  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  explicit Table(Rep* rep) : rep_(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  Status InternalGet(const ReadOptions&, const Slice& key, vector<LeafKey>& leafKeys);

  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);




  Rep* const rep_;

 public:
  class ST_finder{
   public:
#if istime == 2
    ST_finder(Table* rep, saxt_only saxt_, ts_time time1, ts_time  time2, ts_type* paa_) : rep_(rep->rep_) {
      leafkey = saxt_;
      startTime = time1;
      endTime = time2;
      memcpy(paa, paa_, sizeof(ts_type) * Segments);
    }
#else

    ST_finder(Table* rep, saxt_only saxt_, ts_type* paa_) : rep_(rep->rep_), isdel(true) {
      leafkey = saxt_;
      memcpy(paa, paa_, sizeof(ts_type) * Segments);
    }
#endif

    ~ST_finder() { if(isdel) delete to_find_nonleaf; }

   public:

    void root_Get();

    void find_One(LeafKey* res, int& res_num);

    void find_One(LeafKey* res, int& res_num, int id);

    void sort();

   private:

    STLeaf* getSTLeaf(STNonLeaf& nonLeaf, int i);
    STNonLeaf* getSTNonLeaf(STNonLeaf& nonLeaf, int i);
    inline int whereofKey(saxt lsaxt, saxt rsaxt, saxt leafKey, cod co_d);
    bool saxt_cmp(saxt a, saxt b, cod co_d);
    cod get_co_d_from_saxt(saxt a, saxt b, cod pre_d);
    void l_Get_NonLeaf(STNonLeaf& nonLeaf1, int i);

    void r_Get_NonLeaf(STNonLeaf& nonLeaf1, int i);

    int leaf_Get(STLeaf& leaf, LeafKey* res);

    int leaf_Get1(STLeaf& leaf, LeafKey* res, int nownum);

// 在nonLeaf的范围里, 才调用
    bool nonLeaf_Get(STNonLeaf& nonLeaf);

   public:
    vector<pair<float, int>> has_cod;
    vector<int> no_has_cod;
   private:
    saxt_only leafkey;
#if istime == 2
    ts_time startTime;
    ts_time endTime;
#endif
    ts_type paa[Segments];
    STNonLeaf* to_find_nonleaf;
    bool isdel;
    int oneId;//叶子在非叶结点中的位置
    Rep* const rep_;
  };

  class ST_finder_exact {
   public:

    ST_finder_exact(Table* rep, aquery* aquery_, query_heap_exact* heap_, const void* gs_jvm, bool isbig, ThreadPool* pool_get): rep_(rep->rep_),aquery1(aquery_), heap(heap_), gs_jvm(gs_jvm), isbig(isbig), pool_get(pool_get) {
      charcpy(add_info, &aquery1->rep, sizeof(aquery_rep));
      charcpy(add_info, &aquery1->k, sizeof(int));
      tmp_distp.reserve(Leaf_rebuildnum);
      tmp_distp.resize(Leaf_rebuildnum);
      if(isbig) {
        tmp_pool = new ThreadPool(18);
      }
    }

    ST_finder_exact(ST_finder_exact* finder){
//      cout<<"ll"<<endl;
      gs_jvm = finder->gs_jvm;
      rep_ = finder->rep_;
      isbig = finder->isbig;
      pool_get = finder->pool_get;
      aquery1 = finder->aquery1;
      heap = finder->heap;
      topdist = finder->topdist;
      need = finder->need;
      memcpy(info, finder->info, sizeof(aquery_rep) + sizeof(int)*3 + sizeof(float) + sizeof(void*) * Leaf_rebuildnum);
      add_info = finder->add_info;
      tmp_distp.reserve(Leaf_rebuildnum);
      tmp_distp.resize(Leaf_rebuildnum);
    }

    void start();




    void nonLeaf_dfs(STNonLeaf& nonLeaf);
   protected:

    STLeaf* getSTLeaf(STNonLeaf& nonLeaf, int i);
    STNonLeaf* getSTNonLeaf(STNonLeaf& nonLeaf, int i);
    virtual void leaf_todo(STLeaf& leaf);

    virtual void root_dfs(STNonLeaf& nonLeaf);


    vector<pair<float, void*>> tmp_distp;
    ThreadPool* tmp_pool;
    bool isbig;
    ThreadPool* pool_get;
    aquery* aquery1;
    query_heap_exact* heap;
    float topdist;
    const void* gs_jvm;
    int need;
    char info[sizeof(aquery_rep) + sizeof(int)*3 + sizeof(float) + sizeof(void*) * Leaf_rebuildnum];
    char* add_info = info;
    const Rep* rep_;
#if iscount_saxt_for_exact
   public:
    uint64_t saxt_num = 0;
#endif
  };

  class ST_finder_exact_appro : public ST_finder_exact{
   public:
    ST_finder_exact_appro(Table* rep, aquery* aquery,
                          query_heap_exact* heap, const void* gsJvm, bool isbig, ThreadPool* pool_get)
        : ST_finder_exact(rep,aquery,heap,gsJvm,isbig,pool_get) {}


    ST_finder_exact_appro(ST_finder_exact_appro* finder)
        : ST_finder_exact(finder) {}



   private:
    void leaf_todo(STLeaf& leaf) override ;
    void root_dfs(STNonLeaf& nonLeaf) override ;

  };


  class ST_Iter{
   public:
    ST_Iter(Table* table);
    bool next(LeafKey& res);
    void setPrefix(LeafKey& res);
    ~ST_Iter();
   private:
    bool getSTLeaf();
    void getSTNonLeaf();
    Rep* const rep_;
    //栈，要delete除了root
    vector<STNonLeaf*> st_nonleaf_stack;
    //指定当前的位置
    int top;
    //指定当前nonleaf的nonleafkey位置
    int nonleaftops[30];
    STLeaf *stLeaf;
    int leaftop;
  };


};

static void ST_finder_exact_multi(Table::ST_finder_exact* finder, STNonLeaf* todoSTNonLeaf) {
  finder->nonLeaf_dfs(*todoSTNonLeaf);

  //这是malloc的
  delete finder;
  delete todoSTNonLeaf;
}

static void ST_finder_exact_appro_multi(Table::ST_finder_exact_appro* finder, STNonLeaf* todoSTNonLeaf) {
  finder->nonLeaf_dfs(*todoSTNonLeaf);
  //这是malloc的
  delete finder;
  delete todoSTNonLeaf;
}



}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
