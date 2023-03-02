//
// Created by hh on 2022/12/26.
//

#include "leveldb_sax_db.h"
#include "threadPool_2.h"
#include "leveldb/db.h"
#include "db/query_heap.h"
#include "newVector.h"

static leveldb::DB* db;
static leveldb::WriteOptions writeOptions;
static leveldb::ReadOptions readOptions;
static JavaVM* gs_jvm;
//static ThreadPool *pool;

JNIEXPORT void JNICALL Java_leveldb_1sax_db_print_1time
    (JNIEnv *, jobject) {
#if iscompaction_time
  cout<<"压缩合并时间: "<<db->getTime()/1000<<endl;
#endif
}

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


JNIEXPORT jlong JNICALL Java_leveldb_1sax_db_put_1buffer
    (JNIEnv *env, jobject, jint tskeynum, jobject ts_buffer, jobject leaftimekeys_buffer, jint hash, jlong offset) {
  tsKey* tskeys_c = (tsKey*)env->GetDirectBufferAddress(ts_buffer);
  LeafTimeKey* leafTimeKeys = (LeafTimeKey*)env->GetDirectBufferAddress(leaftimekeys_buffer);
  long stime = 0;
#if iscompaction_time
  std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
#endif
  for(int i=0;i<tskeynum;i++) {
    saxt_from_ts(tskeys_c[i].ts, leafTimeKeys[i].leafKey.asaxt.asaxt);
#if ishash
    leafTimeKeys[i].leafKey.p = (void*) ( (offset + i) | ((uint64_t)hash) << 56);
#else
    leafTimeKeys[i].leafKey.p = (void*) (offset + i);

#endif
#if istime == 1
    leafTimeKeys[i].keytime = tskeys_c[i].tsTime;
#elif istime == 2
    leafTimeKeys[i].leafKey.keytime_ = tskeys_c[i].tsTime;
#endif
  }
#if iscompaction_time
  std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
  stime = std::chrono::duration_cast<std::chrono::microseconds>( t2-t1 ).count();
#endif
  for(int i=0;i<tskeynum;i++) db->Put(writeOptions, leafTimeKeys[i]);

  return stime;
}

JNIEXPORT void JNICALL Java_leveldb_1sax_db_init_1buffer
    (JNIEnv *env, jobject, jint tskeynum, jobject ts_buffer, jobject leaftimekeys_buffer, jint hash, jlong offset) {
  tsKey* tskeys_c = (tsKey*)env->GetDirectBufferAddress(ts_buffer);
  LeafTimeKey* leafTimeKeys = (LeafTimeKey*)env->GetDirectBufferAddress(leaftimekeys_buffer);

  for(int i=0;i<tskeynum;i++) {
    saxt_from_ts(tskeys_c[i].ts, leafTimeKeys[i].leafKey.asaxt.asaxt);
#if ishash
    leafTimeKeys[i].leafKey.p = (void*) ( (offset + i) | ((uint64_t)hash) << 56);
#else
    leafTimeKeys[i].leafKey.p = (void*) (offset + i);

#endif
#if istime == 1
    leafTimeKeys[i].keytime = tskeys_c[i].tsTime;
#elif istime == 2
    leafTimeKeys[i].leafKey.keytime_ = tskeys_c[i].tsTime;
#endif
  }
  sort(leafTimeKeys, leafTimeKeys + tskeynum);

  db->Init(leafTimeKeys, tskeynum);
}

JNIEXPORT void JNICALL Java_leveldb_1sax_db_init_1buffer1
    (JNIEnv *env, jobject, jint tskeynum, jobject ts_buffer, jint tskeynum1, jobject ts_buffer1, jobject leaftimekeys_buffer, jint hash, jlong offset) {

  tsKey* tskeys_c = (tsKey*)env->GetDirectBufferAddress(ts_buffer);
  tsKey* tskeys_c1 = (tsKey*)env->GetDirectBufferAddress(ts_buffer1);
  LeafTimeKey* leafTimeKeys = (LeafTimeKey*)env->GetDirectBufferAddress(leaftimekeys_buffer);

  for(int i=0;i<tskeynum;i++) {
    saxt_from_ts(tskeys_c[i].ts, leafTimeKeys[i].leafKey.asaxt.asaxt);
#if ishash
    leafTimeKeys[i].leafKey.p = (void*) ( (offset + i) | ((uint64_t)hash) << 56);
#else
    leafTimeKeys[i].leafKey.p = (void*) (offset + i);

#endif
#if istime == 1
    leafTimeKeys[i].keytime = tskeys_c[i].tsTime;
#elif istime == 2
    leafTimeKeys[i].leafKey.keytime_ = tskeys_c[i].tsTime;
#endif
  }
  int num = tskeynum + tskeynum1;
  for(int i=tskeynum, j=0;j<tskeynum1 && i < num ;i++, j++) {
    saxt_from_ts(tskeys_c1[j].ts, leafTimeKeys[i].leafKey.asaxt.asaxt);
#if ishash
    leafTimeKeys[i].leafKey.p = (void*) ( (offset + i) | ((uint64_t)hash) << 56);
#else
    leafTimeKeys[i].leafKey.p = (void*) (offset + i);

#endif
#if istime == 1
    leafTimeKeys[i].keytime = tskeys_c1[j].tsTime;
#elif istime == 2
    leafTimeKeys[i].leafKey.keytime_ = tskeys_c1[j].tsTime;
#endif
  }


  sort(leafTimeKeys, leafTimeKeys + num);

  db->Init(leafTimeKeys, num);
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

JNIEXPORT void JNICALL Java_leveldb_1sax_db_paa_1saxt_1from_1ts_1buffer
    (JNIEnv *env, jobject, jobject ts, jobject saxt_out, jobject paa_out) {
  ts_only* ts_ = (ts_only*)env->GetDirectBufferAddress(ts);
  saxt_only* saxt_out_ = (saxt_only*)env->GetDirectBufferAddress(saxt_out);
  paa_only* paa_out_ = (paa_only*)env->GetDirectBufferAddress(paa_out);
  paa_saxt_from_ts((ts_type*)ts_, (saxt_type*)saxt_out_, (ts_type*)paa_out_);
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

JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_dist_1ts_1buffer
    (JNIEnv *env, jobject, jobject ts1, jobject ts2) {
  ts_type* ts1_ = (ts_type*)env->GetDirectBufferAddress(ts1);
  ts_type* ts2_ = (ts_type*)env->GetDirectBufferAddress(ts2);
  return ts_euclidean_distance(ts1_, ts2_, Ts_length);
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

JNIEXPORT jint JNICALL Java_leveldb_1sax_db_Get
    (JNIEnv *env, jobject, jobject aquery_, jboolean is_use_am, jint am_version_id, jint st_version_id,
     jint st_number_num, jobject st_number, jobject ress, jobject info) {
  int res_amid = -1;
  aquery* aquery1 = (aquery*)env->GetDirectBufferAddress(aquery_);
  uint64_t* st_numbers_ = (uint64_t*)env->GetDirectBufferAddress(st_number);
  ares* results_ = (ares*)env->GetDirectBufferAddress(ress);
  jniVector<ares> results(results_, 0);
  jniVector<uint64_t> st_numbers(st_numbers_, st_number_num);
  char* info_ = (char*)env->GetDirectBufferAddress(info);
  jniInfo info1 = {info_, info};
  db->Get(*aquery1, is_use_am, am_version_id, st_version_id, st_numbers, results, res_amid, info1);
  int* a = (int*)(results_ + results.size());
  *a = res_amid;
  return results.size();
}

JNIEXPORT jint JNICALL Java_leveldb_1sax_db_Get_1exact
    (JNIEnv *env, jobject, jobject aquery_, jint am_version_id, jint st_version_id,
     jint st_number_num, jobject st_number, jint appro_res_num, jobject appro_res,
     jint appro_st_number_num, jobject appro_st_number, jobject exact_res, jobject info) {
  int appro_am_id = -1;
  aquery* aquery1 = (aquery*)env->GetDirectBufferAddress(aquery_);
  uint64_t* st_numbers_ = (uint64_t*)env->GetDirectBufferAddress(st_number);
  jniVector<uint64_t> st_numbers(st_numbers_, st_number_num);
  uint64_t* appro_st_number_ = (uint64_t*)env->GetDirectBufferAddress(appro_st_number);
  jniVector<uint64_t> appro_st_numbers(appro_st_number_, appro_st_number_num);
  ares* appro_res_ = (ares*)env->GetDirectBufferAddress(appro_res);
  appro_am_id = *((int*)(appro_res_ + appro_res_num));
  jniVector<ares> appro(appro_res_, appro_res_num);
  ares_exact* exact_res_ = (ares_exact*)env->GetDirectBufferAddress(exact_res);
  jniVector<ares_exact> results(exact_res_, 0);
  char* info_ = (char*)env->GetDirectBufferAddress(info);
  jniInfo info1 = {info_, info};
  db->Get_exact(*aquery1, am_version_id, st_version_id, st_numbers, appro, results, appro_am_id, appro_st_numbers, info1);

  return results.size();
}


JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push_1buffer
    (JNIEnv *env, jobject, jobject ares_b, jbyteArray heap_b) {
  leveldb::query_heap* heap1;
  ares* ares1 = (ares*)env->GetDirectBufferAddress(ares_b);
  env->GetByteArrayRegion(heap_b, 0, sizeof(void*), (jbyte*)&heap1);
  return heap1->push2(*ares1);
}


JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push_1exact_1buffer
    (JNIEnv *env, jobject, jobject ares_exact_b, jbyteArray heap_b) {
  leveldb::query_heap_exact* heap1;
  ares_exact* ares1 = (ares_exact*)env->GetDirectBufferAddress(ares_exact_b);
  env->GetByteArrayRegion(heap_b, 0, sizeof(void*), (jbyte*)&heap1);
  return heap1->push2(*ares1);
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
