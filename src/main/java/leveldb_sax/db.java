package leveldb_sax;

import java.io.IOException;
import java.nio.ByteBuffer;

public class db {

    static {
        System.loadLibrary("leveldbj");
    }
    //sax

    public native void print_time();

//    @Deprecated
//    public native byte[] leaftimekey_from_tskey(byte[] tskeys, int hash, long offset, boolean issort);
//
//    @Deprecated
//    public native void leaftimekey_sort(byte[] leaftimekeys);

    /**
     * leaftimekeys 和 tskeys 要一样多，不然出错
     * @param tskeys
     * @param leaftimekeys
     * @param hash
     * @param offset
     */
    public native long put_buffer(int tskeys_num, ByteBuffer tskeys, ByteBuffer leaftimekeys, int hash, long offset);

    /**
     * 初始化需要的数目
     * @param tskeys_num 总数
     */
    public native void init_malloc(int tskeys_num);

    public native void init_putbuffer(int tskeys_num, int this_num, ByteBuffer tskeys, int hash, long offset);

    /**
     * 整个初始化放入完后，最后调用
     * @param tskeys_num 总数
     */
    public native void init_finish(int tskeys_num);

//    /**
//     * leaftimekeys 和 tskeys 要一样多，不然出错
//     */
//    @Deprecated
//    public native void init_buffer(int tskeys_num, ByteBuffer tskeys, ByteBuffer leaftimekeys, int hash, long offset);
//    @Deprecated
//    public native void init_buffer1(int tskeys_num, ByteBuffer tskeys, int tskeys_num1, ByteBuffer tskeys1, ByteBuffer leaftimekeys, int hash, long offset);

//    @Deprecated
//    public native void saxt_from_ts(byte[] ts, byte[] saxt);


//    @Deprecated
    public native void paa_saxt_from_ts(byte[] ts, byte[] saxt, float[] paa);
    public native void paa_saxt_from_ts_buffer(ByteBuffer ts, ByteBuffer saxt, ByteBuffer paa);



    //db
    public native void open(String dbname);

    public native void close();


//    @Deprecated
//    public native void init(byte[] leafTimeKeys, int leafKeysNum);

//    @Deprecated
//    public native void put(byte[] leafTimeKey);

//    @Deprecated
    public native byte[] Get(byte[] aquery, boolean is_use_am, int am_version_id, int st_version_id, long[] st_number);
    // st_number, 选择的sstable
    // aquery(有时间戳): ts 256*4, startTime 8, endTime 8, k 4，paa 4*paa大小, saxt 8/16, 空4位(因为time是long,需对齐)
    // aquery(没时间戳): ts 256*4, k 4，paa 4*paa大小, saxt 8/16

    // appro_res(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐), p 8
    // appro_res(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), p 8
    // info(没时间戳): ts 256*4, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n, n要根据c++的参数来开
    // info(有时间戳): ts 256*4，starttime 8， endtime 8, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n, n要根据c++的参数来开
    // appro_res, info 是java分配的空间
    // Get写入若干个appro_res,appro_res的最后有一个4字节的id,用于标记近似查的是当前am版本中的哪个表(一个am版本有多个表并行维护不同的saxt树),用于精准查询的appro_res(去重)
    // Get返回appro_res的数量
    public native int Get(ByteBuffer aquery, boolean is_use_am, int am_version_id, int st_version_id,
                          int st_number_num, ByteBuffer st_number, ByteBuffer appro_res, ByteBuffer info);


//    @Deprecated
    public native byte[] Get_exact(byte[] aquery, int am_version_id, int st_version_id, long[] st_number, byte[] appro_res, long[] appro_num);
    // exact_res(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐)
    // exact_res(有时间戳): ts 256*4, float dist 4
    // Get_exact返回若干个exact_res, 这个exact_res没有p也不用空4位
    //返回数量
    public native int Get_exact(ByteBuffer aquery, int am_version_id, int st_version_id,
                                        int st_number_num, ByteBuffer st_number, int appro_res_num, ByteBuffer appro_res,
                                       int appro_st_number_num, ByteBuffer appro_st_number, ByteBuffer exact_res, ByteBuffer info);




    // 向堆push ts，ares里只含一个ts，push后c返回topdis, 更新topdis
    public native float heap_push_buffer(ByteBuffer ares, byte[] heap);
    public native float heap_push_exact_buffer(ByteBuffer ares_exact, byte[] heap);

    // 向堆push ts，ares里只含一个ts，push后c返回topdis, 更新topdis
//    @Deprecated
//    public native float heap_push(byte[] ares, byte[] heap);
//    @Deprecated
//    public native float heap_push_exact(byte[] ares_exact, byte[] heap);
    public native void unref_am(int version_id);

    public native void unref_st(int version_id);

//    @Deprecated
    public native float dist_ts(byte[] ts1, byte[] ts2);
    public native float dist_ts_buffer(ByteBuffer ts1, ByteBuffer ts2);

    public static void main(String[] args) throws IOException, InterruptedException {

        ByteBuffer a = ByteBuffer.allocateDirect(10);


    }
}