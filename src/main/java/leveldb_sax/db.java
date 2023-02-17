package leveldb_sax;

import com.local.util.DBUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class db {

    static {
        System.loadLibrary("leveldbj");
    }
    //sax
    public native byte[] leaftimekey_from_tskey(byte[] tskeys, int hash, long offset, boolean issort);
    public native void leaftimekey_sort(byte[] leaftimekeys);

    public native void saxt_from_ts(byte[] ts, byte[] saxt);

    public native void paa_saxt_from_ts(byte[] ts, byte[] saxt, float[] paa);

    //db
    public native void open(String dbname);

    public native void close();

    public native void init(byte[] leafTimeKeys, int leafKeysNum);

    public native void put(byte[] leafTimeKey);


    // st_number, 选择的sstable
    // aquery(有时间戳): ts 256*4, startTime 8, endTime 8, k 4，paa 4*paa大小, saxt 8/16, 空4位(因为time是long,需对齐)
    // aquery(没时间戳): ts 256*4, k 4，paa 4*paa大小, saxt 8/16

    // ares(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐), p 8
    // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), p 8
    // Get返回若干个ares,ares的最后有一个4字节的id,用于标记近似查的是当前am版本中的哪个表(一个am版本有多个表并行维护不同的saxt树),用于精准查询的appro_res(去重)
    public native byte[] Get(byte[] aquery, boolean is_use_am, int am_version_id, int st_version_id, long[] st_number);


    // ares_exact(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐)
    // ares_exact(有时间戳): ts 256*4, float dist 4
    // Get_exact返回若干个ares_exact, 这个ares_exact没有p也不用空4位
    public native byte[] Get_exact(byte[] aquery, int am_version_id, int st_version_id, long[] st_number, byte[] appro_res, long[] appro_st_number);

    // 向堆push ts，ares里只含一个ts，push后c返回topdis, 更新topdis
    public native float heap_push(byte[] ares, byte[] heap);
    public native float heap_push_exact(byte[] ares_exact, byte[] heap);
    public native void unref_am(int version_id);

    public native void unref_st(int version_id);
    public native float dist_ts(byte[] ts1, byte[] ts2);

    public static void main(String[] args) throws IOException, InterruptedException {

//        class PutRun implements Runnable{
//            public PutRun(db d, byte[] leafTimeKey) { this.d = d; this.leafTimeKey = leafTimeKey;}
//            public void run() {
//                d.put(leafTimeKey);
//            }
//            private final db d;
//            private final byte[] leafTimeKey;
//        }

//        ThreadPoolExecutor pool = new ThreadPoolExecutor(20, 50, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
//                Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());

        int saxt_num = 1000000, saxt_num1 = 2000000;
        FileInputStream input = new FileInputStream("saxt6.bin");
        byte[] leaftimekeys = new byte[24*saxt_num];
        byte[] saxts = new byte[16];
        for (int i=0;i<saxt_num;i++) {
            input.read(saxts);
            System.arraycopy(saxts, 0, leaftimekeys, i*24, 16);
        }

//        FileInputStream input1 = new FileInputStream("saxt.bin");
//        byte[] leaftimekeys1 = new byte[24*saxt_num1];
//        byte[] saxts1 = new byte[16];
//        for (int i=0; i < saxt_num1;i++) {
//            input1.read(saxts1);
//            System.arraycopy(saxts1, 0, leaftimekeys1, i*24, 16);
////            System.out.println(i);
//        }

        db d = new db();
        d.open("./testdb");
        d.init(leaftimekeys, saxt_num);
//        Thread.sleep(1000);
        long stime = System.currentTimeMillis();

//        d.put(leaftimekeys1);
//        d.put(leaftimekeys1);
//        d.put(leaftimekeys1);
//        d.put(leaftimekeys1);
        for (int i = 0; i < 1e6; i ++ ) {
            byte[] test = new byte[24];
            Random r = new Random();
            r.nextBytes(test);
            System.out.println(Arrays.toString(test));
            DBUtil.dataBase.put(test);
            System.out.println(i);
        }

        long etime = System.currentTimeMillis();
        System.out.printf("执行时长：%d 毫秒.", (etime - stime));

        Thread.sleep(5000);
        System.out.println("over");
        d.close();
    }
}