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

    // aquery
    // ts 256*4, startTime 8, endTime 8, k 4，paa 32, saxt 8 , 空4位 一共1088
    // st_number
    // 选择的sstable
    // 返回至多k个ares

    //ares
    //    ts 256*4 time 8
    //    float dist 4 最后4位为空
    //    1040

    // Get和Get_exact返回若干个ares
    public native byte[] Get(byte[] aquery, boolean is_use_am, int am_version_id, int st_version_id, long[] st_number);
    public native byte[] Get_exact(byte[] aquery, int am_version_id, int st_version_id, long[] st_number, byte[] appro_res);
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