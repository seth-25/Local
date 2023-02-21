//
// Created by hh on 2022/12/26.
//

#include "leveldb_sax_db.h"
#include "threadPool_2.h"
#include "leveldb/db.h"
#include "db/query_heap.h"

static leveldb::DB* db;
static leveldb::WriteOptions writeOptions;
static leveldb::ReadOptions readOptions;
static JavaVM* gs_jvm;
//static ThreadPool *pool;

JNIEXPORT jbyteArray JNICALL Java_leveldb_1sax_db_leaftimekey_1from_1tskey
    (JNIEnv *env, jobject, jbyteArray tskeys, jint hash, jlong offset, jboolean issort) {
  size_t size = env->GetArrayLength(tskeys);
  size_t tskeynum  = size / sizeof(tsKey);
  tsKey* tskeys_c = (tsKey*) malloc(size);
  size_t leaftimekey_size = tskeynum*sizeof(LeafTimeKey);
  LeafTimeKey* leafTimeKeys = (LeafTimeKey*)malloc(leaftimekey_size);
  env->GetByteArrayRegion(tskeys, 0, size, (jbyte*)tskeys_c);
  for(int i=0;i<tskeynum;i++) {
    saxt_from_ts(tskeys_c[i].ts, leafTimeKeys[i].leafKey.asaxt.asaxt);
    leafTimeKeys[i].leafKey.p = (void*) ( (offset + i) | ((uint64_t)hash) << 56);
#if istime == 1
    leafTimeKeys[i].keytime = tskeys_c[i].tsTime;
#elif istime == 2
    leafTimeKeys[i].leafKey.keytime_ = tskeys_c[i].tsTime;
#endif
  }
  if (issort) {
    sort(leafTimeKeys, leafTimeKeys + tskeynum);
  }
  jbyteArray newArr = env->NewByteArray(leaftimekey_size);
  env->SetByteArrayRegion(newArr, 0, leaftimekey_size, (jbyte*)leafTimeKeys);
  free(tskeys_c);
  free(leafTimeKeys);
  return newArr;
}

JNIEXPORT void JNICALL Java_leveldb_1sax_db_leaftimekey_1sort
    (JNIEnv *env, jobject, jbyteArray leafs) {
  size_t size = env->GetArrayLength(leafs);
  size_t leaftimekeysnum  = size / sizeof(LeafTimeKey);
  LeafTimeKey* leafTimeKeys = (LeafTimeKey*)malloc(size);
  env->GetByteArrayRegion(leafs, 0, size, (jbyte*)leafTimeKeys);

  sort(leafTimeKeys, leafTimeKeys + leaftimekeysnum);

  env->SetByteArrayRegion(leafs, 0, size, (jbyte*)leafTimeKeys);
  free(leafTimeKeys);
}


JNIEXPORT void JNICALL Java_leveldb_1sax_db_saxt_1from_1ts
    (JNIEnv *env, jobject, jbyteArray ts, jbyteArray saxt_out) {

  ts_only tsArr;
  saxt_only saxtArr;
//  memset(saxtArr.asaxt, 0, sizeof(saxt_only));
  env->GetByteArrayRegion(ts, 0, sizeof(ts_only), (jbyte*)tsArr.ts);
  saxt_from_ts(tsArr.ts, saxtArr.asaxt);
//  jbyteArray newArr = env->NewByteArray(sizeof(saxt_only));
//  env->SetByteArrayRegion(newArr, 0, sizeof(saxt_only), (jbyte*)saxtArr.asaxt);
  env->SetByteArrayRegion(saxt_out, 0, sizeof(saxt_only), (jbyte*)saxtArr.asaxt);
//  return newArr;
}

JNIEXPORT void JNICALL Java_leveldb_1sax_db_paa_1saxt_1from_1ts
    (JNIEnv *env, jobject, jbyteArray ts, jbyteArray saxt_out, jfloatArray paa_out) {
  ts_only tsArr;
  saxt_only saxtArr;
//  memset(saxtArr.asaxt, 0, sizeof(saxt_only));
  paa_only paaArr;

  env->GetByteArrayRegion(ts, 0, sizeof(ts_only), (jbyte*)tsArr.ts);
  paa_saxt_from_ts(tsArr.ts, saxtArr.asaxt, paaArr.apaa);
  env->SetByteArrayRegion(saxt_out, 0, sizeof(saxt_only), (jbyte*)saxtArr.asaxt);
  env->SetFloatArrayRegion(paa_out, 0, Segments, paaArr.apaa);
}

JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_dist_1ts
    (JNIEnv *env, jobject, jbyteArray ts1, jbyteArray ts2) {
  ts_only tsArr1, tsArr2;
  env->GetByteArrayRegion(ts1, 0, sizeof(ts_only), (jbyte*)tsArr1.ts);
  env->GetByteArrayRegion(ts2, 0, sizeof(ts_only), (jbyte*)tsArr2.ts);
  return ts_euclidean_distance(tsArr1.ts, tsArr2.ts, Ts_length);
}


JNIEXPORT void JNICALL Java_leveldb_1sax_db_open
    (JNIEnv *env, jobject, jstring dbname) {
//  pool = new ThreadPool(30);
  //把java环境拿到
  env->GetJavaVM(&gs_jvm);
  const char* str;
  jboolean isCopy;
  str = env->GetStringUTFChars(dbname, &isCopy);
  if(str == nullptr) return;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, str, gs_jvm, &db);
  env->ReleaseStringUTFChars(dbname, str);
  assert(status.ok());
}


JNIEXPORT void JNICALL Java_leveldb_1sax_db_close
    (JNIEnv *, jobject) {
  delete db;
}


JNIEXPORT void JNICALL Java_leveldb_1sax_db_init
    (JNIEnv *env, jobject, jbyteArray leafTimeKeys, jint leafKeysNum) {
  int len = sizeof(LeafTimeKey)*leafKeysNum;
  LeafTimeKey* data = (LeafTimeKey*)malloc(len);
  env->GetByteArrayRegion(leafTimeKeys, 0, len, (jbyte*)data);
  db->Init(data, leafKeysNum);
  free(data);
}


JNIEXPORT void JNICALL Java_leveldb_1sax_db_put
    (JNIEnv *env, jobject, jbyteArray leafTimeKey) {
  size_t size = env->GetArrayLength(leafTimeKey);
  LeafTimeKey *key = (LeafTimeKey*)malloc(size);
  env->GetByteArrayRegion(leafTimeKey, 0, size, (jbyte*)key);
//  pool->enqueue(std::bind(&leveldb::DB::Put, db, writeOptions, key));
  int num = size / sizeof(LeafTimeKey);
  for(int i=0;i<num;i++) db->Put(writeOptions, key[i]);
  free(key);
}

JNIEXPORT jbyteArray JNICALL Java_leveldb_1sax_db_Get
    (JNIEnv *env, jobject, jbyteArray aquery_, jboolean is_use_am, jint am_version_id, jint st_version_id, jlongArray st_number) {
  int res_amid = -1;
  aquery aquery1;
  vector<uint64_t> st_numbers;
  vector<ares> results;
  env->GetByteArrayRegion(aquery_, 0, sizeof(aquery), (jbyte*)&aquery1);
  size_t len = env->GetArrayLength(st_number);
  if (len) {
    st_numbers.reserve(len);
    st_numbers.resize(len);
    env->GetLongArrayRegion(st_number, 0, len, (jlong*)st_numbers.data());
  }
  db->Get(aquery1, is_use_am, am_version_id, st_version_id, st_numbers, results, res_amid);
  jbyteArray newArr = env->NewByteArray(results.size()*sizeof(ares) + sizeof(int));
  env->SetByteArrayRegion(newArr, 0, results.size()*sizeof(ares), (jbyte*)results.data());
  env->SetByteArrayRegion(newArr, results.size()*sizeof(ares), sizeof(int), (jbyte*)&res_amid);
  return newArr;
}

JNIEXPORT jbyteArray JNICALL Java_leveldb_1sax_db_Get_1exact
    (JNIEnv *env, jobject, jbyteArray aquery_, jint am_version_id, jint st_version_id, jlongArray st_number,
     jbyteArray appro_res, jlongArray appro_st_number) {
  int appro_am_id = -1;
  aquery aquery1;
  vector<uint64_t> st_numbers;
  vector<uint64_t> appro_st_numbers;
  vector<ares_exact> results;
  vector<ares> appro;
  env->GetByteArrayRegion(aquery_, 0, sizeof(aquery), (jbyte*)&aquery1);
  size_t len = env->GetArrayLength(st_number);
  if (len) {
    st_numbers.reserve(len);
    st_numbers.resize(len);
    env->GetLongArrayRegion(st_number, 0, len, (jlong*)st_numbers.data());
  }
  len = env->GetArrayLength(appro_st_number);
  if (len) {
    appro_st_numbers.reserve(len);
    appro_st_numbers.resize(len);
    env->GetLongArrayRegion(appro_st_number, 0, len, (jlong*)appro_st_numbers.data());
  }
  len = env->GetArrayLength(appro_res);
  len -= sizeof(int);
  if (len) {
    appro.reserve(len / sizeof(ares));
    appro.resize(len / sizeof(ares));
    env->GetByteArrayRegion(appro_res, 0, len, (jbyte*)appro.data());
    env->GetByteArrayRegion(appro_res, len, sizeof(int), (jbyte*)&appro_am_id);
  }
  db->Get_exact(aquery1, am_version_id, st_version_id, st_numbers, appro, results, appro_am_id, appro_st_numbers);
  jbyteArray newArr = env->NewByteArray(results.size()*sizeof(ares_exact));
  env->SetByteArrayRegion(newArr, 0, results.size()*sizeof(ares_exact), (jbyte*)results.data());
  return newArr;
}

JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push
    (JNIEnv *env, jobject, jbyteArray ares_b, jbyteArray heap_b) {
  leveldb::query_heap* heap1;
  ares ares1;
  env->GetByteArrayRegion(ares_b, 0, sizeof(ares), (jbyte*)&ares1);
  env->GetByteArrayRegion(heap_b, 0, sizeof(void*), (jbyte*)&heap1);

  return heap1->push2(ares1);

}

JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push_1exact
    (JNIEnv *env, jobject, jbyteArray ares_exact_b, jbyteArray heap_b) {
  leveldb::query_heap_exact* heap1;
  ares_exact ares1;
  env->GetByteArrayRegion(ares_exact_b, 0, sizeof(ares), (jbyte*)&ares1);
  env->GetByteArrayRegion(heap_b, 0, sizeof(void*), (jbyte*)&heap1);

  return heap1->push2(ares1);

}

JNIEXPORT void JNICALL Java_leveldb_1sax_db_unref_1am
    (JNIEnv *, jobject, jint id) {
  db->UnRef_am(id);
}


JNIEXPORT void JNICALL Java_leveldb_1sax_db_unref_1st
    (JNIEnv *, jobject, jint id) {
  db->UnRef_st(id);
}
