/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class leveldb_sax_db */

#ifndef _Included_leveldb_sax_db
#define _Included_leveldb_sax_db
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     leveldb_sax_db
 * Method:    print_time
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_print_1time
  (JNIEnv *, jobject);

/*
 * Class:     leveldb_sax_db
 * Method:    leaftimekey_from_tskey
 * Signature: ([BIJZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_leveldb_1sax_db_leaftimekey_1from_1tskey
  (JNIEnv *, jobject, jbyteArray, jint, jlong, jboolean);

/*
 * Class:     leveldb_sax_db
 * Method:    leaftimekey_sort
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_leaftimekey_1sort
  (JNIEnv *, jobject, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    put_buffer
 * Signature: (ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;IJ)J
 */
JNIEXPORT jlong JNICALL Java_leveldb_1sax_db_put_1buffer
  (JNIEnv *, jobject, jint, jobject, jobject, jint, jlong);

/*
 * Class:     leveldb_sax_db
 * Method:    init_buffer
 * Signature: (ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;IJ)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_init_1buffer
  (JNIEnv *, jobject, jint, jobject, jobject, jint, jlong);

/*
 * Class:     leveldb_sax_db
 * Method:    init_buffer1
 * Signature: (ILjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;IJ)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_init_1buffer1
  (JNIEnv *, jobject, jint, jobject, jint, jobject, jobject, jint, jlong);

/*
 * Class:     leveldb_sax_db
 * Method:    saxt_from_ts
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_saxt_1from_1ts
  (JNIEnv *, jobject, jbyteArray, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    paa_saxt_from_ts_buffer
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_paa_1saxt_1from_1ts_1buffer
  (JNIEnv *, jobject, jobject, jobject, jobject);

/*
 * Class:     leveldb_sax_db
 * Method:    paa_saxt_from_ts
 * Signature: ([B[B[F)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_paa_1saxt_1from_1ts
  (JNIEnv *, jobject, jbyteArray, jbyteArray, jfloatArray);

/*
 * Class:     leveldb_sax_db
 * Method:    open
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_open
  (JNIEnv *, jobject, jstring);

/*
 * Class:     leveldb_sax_db
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_close
  (JNIEnv *, jobject);

/*
 * Class:     leveldb_sax_db
 * Method:    init
 * Signature: ([BI)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_init
  (JNIEnv *, jobject, jbyteArray, jint);

/*
 * Class:     leveldb_sax_db
 * Method:    put
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_put
  (JNIEnv *, jobject, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    Get
 * Signature: ([BZII[J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_leveldb_1sax_db_Get___3BZII_3J
  (JNIEnv *, jobject, jbyteArray, jboolean, jint, jint, jlongArray);

/*
 * Class:     leveldb_sax_db
 * Method:    Get
 * Signature: (Ljava/nio/ByteBuffer;ZIIILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_leveldb_1sax_db_Get__Ljava_nio_ByteBuffer_2ZIIILjava_nio_ByteBuffer_2Ljava_nio_ByteBuffer_2Ljava_nio_ByteBuffer_2
  (JNIEnv *, jobject, jobject, jboolean, jint, jint, jint, jobject, jobject, jobject);

/*
 * Class:     leveldb_sax_db
 * Method:    Get_exact
 * Signature: ([BII[J[B[J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_leveldb_1sax_db_Get_1exact___3BII_3J_3B_3J
  (JNIEnv *, jobject, jbyteArray, jint, jint, jlongArray, jbyteArray, jlongArray);

/*
 * Class:     leveldb_sax_db
 * Method:    Get_exact
 * Signature: (Ljava/nio/ByteBuffer;IIILjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_leveldb_1sax_db_Get_1exact__Ljava_nio_ByteBuffer_2IIILjava_nio_ByteBuffer_2ILjava_nio_ByteBuffer_2ILjava_nio_ByteBuffer_2Ljava_nio_ByteBuffer_2Ljava_nio_ByteBuffer_2
  (JNIEnv *, jobject, jobject, jint, jint, jint, jobject, jint, jobject, jint, jobject, jobject, jobject);

/*
 * Class:     leveldb_sax_db
 * Method:    heap_push_buffer
 * Signature: (Ljava/nio/ByteBuffer;[B)F
 */
JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push_1buffer
  (JNIEnv *, jobject, jobject, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    heap_push_exact_buffer
 * Signature: (Ljava/nio/ByteBuffer;[B)F
 */
JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push_1exact_1buffer
  (JNIEnv *, jobject, jobject, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    heap_push
 * Signature: ([B[B)F
 */
JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push
  (JNIEnv *, jobject, jbyteArray, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    heap_push_exact
 * Signature: ([B[B)F
 */
JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_heap_1push_1exact
  (JNIEnv *, jobject, jbyteArray, jbyteArray);

/*
 * Class:     leveldb_sax_db
 * Method:    unref_am
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_unref_1am
  (JNIEnv *, jobject, jint);

/*
 * Class:     leveldb_sax_db
 * Method:    unref_st
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_leveldb_1sax_db_unref_1st
  (JNIEnv *, jobject, jint);

/*
 * Class:     leveldb_sax_db
 * Method:    dist_ts_buffer
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)F
 */
JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_dist_1ts_1buffer
  (JNIEnv *, jobject, jobject, jobject);

/*
 * Class:     leveldb_sax_db
 * Method:    dist_ts
 * Signature: ([B[B)F
 */
JNIEXPORT jfloat JNICALL Java_leveldb_1sax_db_dist_1ts
  (JNIEnv *, jobject, jbyteArray, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif
