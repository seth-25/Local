//
// Created by hh on 2022/12/17.
//

#include <cassert>
#include <dirent.h>
#include <iostream>
#include <leveldb/db.h>
#include <string>
#include <zconf.h>

#include "sax.h"
#include "threadPool_2.h"
#include "zsbtree_table.h"
#include <chrono>

#define BOOST_NO_EXCEPTIONS
#include <boost/throw_exception.hpp>
void boost::throw_exception(std::exception const & e){
//do nothing
}

#define over(a) std::cout<<a<<" finished"<<std::endl


leveldb::DB* db;
leveldb::Options options;
std::chrono::steady_clock::time_point t1;
std::chrono::steady_clock::time_point t2;


void test_init(vector<LeafTimeKey>& leafKeys){


  db->Init(leafKeys.data(), leafKeys.size());
  over("init");
}


void test_put(vector<LeafTimeKey>& leafKeys){
  leveldb::WriteOptions writeOptions;
  int k=0;
  int amem1 = 1e6;
  for(int i=0;i<1e6*10;i++) {
    for (int j=0;j<2;j++) {
      db->Put(writeOptions, leafKeys[(i + Table_maxnum*j) % amem1]);
    }
  }

  over("put");
}


static void test_put_multithread_unit(vector<LeafTimeKey>& leafKeys, leveldb::DB* db1){
  leveldb::WriteOptions writeOptions;
  int amem1 = 1e6;
  for(int i=0;i<1e6*4;i++) {
      db->Put(writeOptions, leafKeys[i % amem1]);
  }
  over("put");
}


void test_put_multithread(vector<LeafTimeKey>* leafKeys){


  ThreadPool *pool = new ThreadPool(4);
  leveldb::WriteOptions writeOptions;
  t1 = std::chrono::steady_clock::now();

  for (int i=0;i<4;i++) {
    pool->enqueue(&test_put_multithread_unit, leafKeys[i], db);
  }
  delete pool;
  over("put_multithread");
//  ThreadPool pool(16);
//  for(int i=0;i<10;i++) pool.enqueue(std::bind(&ea::add, &aa));
}


static void test_rebalance_small(vector<LeafTimeKey>& leafKeys, int j){
  j %= 2;
  for(int i=600000;i<1000000;i++){
    db->Put(leveldb::WriteOptions(), leafKeys[920000*j+i%30]);
  }
  over("rebalance_small");
}
void test_rebalance_small_multithread(vector<LeafTimeKey>& leafKeys){
  ThreadPool *pool = new ThreadPool(4);
  for (int i=0;i<4;i++) {
    pool->enqueue(&test_rebalance_small, leafKeys, i);
  }
  over("rebalance_small");
}



void test_st_compaction_0(vector<LeafTimeKey>& leafKeys){
  ThreadPool pool(1);
  leveldb::WriteOptions writeOptions;

  for(int i=0;i<10000;i++) {
    for(int j=i;j<leafKeys.size();j+=Table_maxnum)
      pool.enqueue(std::bind(&leveldb::DB::Put, db, writeOptions, leafKeys[j]));
  }
//  leveldb::WriteOptions writeOptions;
//  db->Put(writeOptions, leafKeys[0]);
//  db->Put(writeOptions, leafKeys[200000]);
  over("st_compaction_0");
}

void test_get_am(vector<LeafTimeKey>& leafKeys){
  // 145 235
  saxt_only key = leafKeys[40000].leafKey.asaxt;
  saxt_print(key);
  aquery_rep aqueryRep;
  aquery aquery1;
  aquery1.asaxt = key;
  aquery1.k = 64;
  vector<uint64_t> st_numbers;
  vector<ares> res;
  db->Get(aquery1, true, 1, 1, st_numbers, res);


//  out("size:"+to_string(res.size()));
  over("get_am");
}

void test_get_st(vector<LeafTimeKey>& leafKeys){
  saxt_only key = leafKeys[40000].leafKey.asaxt;
  saxt_print(key);
  aquery_rep aqueryRep;
  aquery aquery1;
  aquery1.asaxt = key;
  aquery1.k = 64;
  vector<uint64_t> st_numbers;
  st_numbers.push_back(4);
  vector<ares> res;
  db->Get(aquery1, false, 1, 2, st_numbers, res);
  over("get_st");
}

int main(){

  // 8 2.30168 + 0.55
  // 7 1.75 + 0.47
  // 6 1.28 + 0.36
  // 5 0.92 + 0.38
  // 4 0.54

  vector<LeafTimeKey> leafKeys;
  leafKeys.reserve(1e6);
  char * filename = "../../data/saxt6.bin";
  FILE * data_file;
  data_file = fopen (filename,"r");
  for(int i=0; i < 1e6; i ++) {
    leafKeys.emplace_back();
    fread(&leafKeys.back().leafKey.p, sizeof(LeafKey) , 1, data_file);
  }
  fclose (data_file);


//  vector<LeafTimeKey> leafKeys_input[4];
//
//  for(int k=0;k<4;k++){
//    leafKeys_input[k].reserve(1e6);
//    string filename_s = "../../data/saxt6"+to_string(k)+".bin";
//    filename = const_cast<char*>(filename_s.c_str());
//    data_file = fopen (filename,"r");
//    for(int i=0; i < 1e6; i ++) {
//      leafKeys_input[k].emplace_back();
//      fread(&leafKeys_input[k][i].leafKey.p, sizeof(LeafKey) , 1, data_file);
//    }
//    fclose (data_file);
//  }

  string dir = "./testdb";

  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, dir, nullptr, &db);

  assert(status.ok());
  test_init(leafKeys);
  sleep(2);
  //一组测试
  t1 = std::chrono::steady_clock::now();
  test_put(leafKeys);
//  test_put_multithread(leafKeys_input);
//  test_rebalance_small(leafKeys, 0);
//  test_rebalance_small_multithread(leafKeys);
////  test_st_compaction_0(leafKeys);
  t2 = std::chrono:: steady_clock::now();
  std::cout <<"一共"<< std::chrono::duration_cast<std::chrono::milliseconds>( t2-t1 ).count() <<"ms==========================================="<< std::endl;
//
//

//  test_get_am(leafKeys_input[0]);
//  test_get_st(leafKeys);
////
  sleep(10);
  out("finished");
  delete db;
}
