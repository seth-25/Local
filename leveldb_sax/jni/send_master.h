//
// Created by hh on 2022/12/30.
//

#ifndef UNTITLED_SEND_MASTER_H
#define UNTITLED_SEND_MASTER_H


#include <jni.h>
#include <chrono>
#include "cstring"
#include <cassert>
#include "globals.h"
#include <cstdlib>
#include "newVector.h"


static inline void charcpy(char*& tmp_p, const void* obj, const size_t size_) {
  memcpy(tmp_p, obj, size_);
  tmp_p += size_;
}

static void send_master(char* a, size_t size_, const void* gs_jvm) {
    //获取环境
    JNIEnv *env;
    assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//    if (gs_jvm->GetEnv((void**)&env, JNI_VERSION_1_6) < 0) {
//      out("211");
//      gs_jvm->AttachCurrentThread((void**)&env, NULL);
//    }
    jclass cls = env->FindClass("leveldb_sax/db_send");
    jmethodID mid = env->GetStaticMethodID(cls, "send_edit", "([B)V");
    jbyteArray newArr = env->NewByteArray(size_);
    env->SetByteArrayRegion(newArr, 0, size_, (jbyte*)a);
    env->CallStaticObjectMethod(cls, mid, newArr);
//  if (isAttached) {
//    sm_playerVM->DetachCurrentThread();
//  }
  env->DeleteLocalRef(newArr);
}


//out 要free, size_out是ares数量
static void find_tskey(char* a, size_t size_, char*& out, size_t& size_out, const void* gs_jvm) {

  //获取环境
  JNIEnv *env;
  assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//
  jclass cls = env->FindClass("leveldb_sax/db_send");
  jmethodID mid = env->GetStaticMethodID(cls, "find_tskey", "([B)[B");

  jbyteArray newArr = env->NewByteArray(size_);
  env->SetByteArrayRegion(newArr, 0, size_, (jbyte*)a);


  jbyteArray resArr = (jbyteArray)(env->CallStaticObjectMethod(cls, mid, newArr));
  size_out = env->GetArrayLength(resArr);
//  out("find_tskey");
//  out1("size_out", size_out);
  out = (char*)malloc(size_out);
  env->GetByteArrayRegion(resArr, 0, size_out, (jbyte*)out);
  size_out /= sizeof(ares);
  env->DeleteLocalRef(resArr);
  env->DeleteLocalRef(newArr);
}

static void find_tskey_exact(char* a, size_t size_, char*& out, size_t& size_out, const void* gs_jvm) {
  //获取环境
  JNIEnv *env;
  assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//
  jclass cls = env->FindClass("leveldb_sax/db_send");
  jmethodID mid = env->GetStaticMethodID(cls, "find_tskey_exact", "([B)[B");

  jbyteArray newArr = env->NewByteArray(size_);
  env->SetByteArrayRegion(newArr, 0, size_, (jbyte*)a);


  jbyteArray resArr = (jbyteArray)(env->CallStaticObjectMethod(cls, mid, newArr));
  size_out = env->GetArrayLength(resArr);
//  out("find_tskey");
//  out1("size_out", size_out);
  out = (char*)malloc(size_out);
  env->GetByteArrayRegion(resArr, 0, size_out, (jbyte*)out);
  size_out /= sizeof(ares_exact);
  env->DeleteLocalRef(resArr);
  env->DeleteLocalRef(newArr);
}


static void find_tskey_ap(char* a, size_t size_, const void* gs_jvm) {

  //获取环境
  JNIEnv *env;
  assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//
  jclass cls = env->FindClass("leveldb_sax/db_send");
  jmethodID mid = env->GetStaticMethodID(cls, "find_tskey_ap", "([B)V");

  jbyteArray newArr = env->NewByteArray(size_);
  env->SetByteArrayRegion(newArr, 0, size_, (jbyte*)a);
  env->CallStaticObjectMethod(cls, mid, newArr);
  env->DeleteLocalRef(newArr);
}

static void find_tskey_exact_ap(char* a, size_t size_, const void* gs_jvm) {
  //获取环境
  JNIEnv *env;
  assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//
  jclass cls = env->FindClass("leveldb_sax/db_send");
  jmethodID mid = env->GetStaticMethodID(cls, "find_tskey_exact_ap", "([B)V");

  jbyteArray newArr = env->NewByteArray(size_);
  env->SetByteArrayRegion(newArr, 0, size_, (jbyte*)a);
  env->CallStaticObjectMethod(cls, mid, newArr);
  env->DeleteLocalRef(newArr);
}

static void find_tskey_ap_buffer(jniInfo jniInfo_, const void* gs_jvm) {

  //获取环境
  JNIEnv *env;
  assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//
  jclass cls = env->FindClass("leveldb_sax/db_send");
  jmethodID mid = env->GetStaticMethodID(cls, "find_tskey_ap_buffer", "(Ljava/nio/ByteBuffer;)V");

  env->CallStaticObjectMethod(cls, mid, (jobject)jniInfo_.bytebuffer_p);

}

static void find_tskey_exact_ap_buffer(jniInfo jniInfo_, const void* gs_jvm) {
  //获取环境
  JNIEnv *env;
  assert(gs_jvm!= nullptr);
  ((JavaVM*)gs_jvm)->AttachCurrentThread((void **)&env, NULL);
//
  jclass cls = env->FindClass("leveldb_sax/db_send");
  jmethodID mid = env->GetStaticMethodID(cls, "find_tskey_exact_ap_buffer", "(Ljava/nio/ByteBuffer;)V");
  env->CallStaticObjectMethod(cls, mid, (jobject)jniInfo_.bytebuffer_p);
}






#endif //UNTITLED_SEND_MASTER_H
