//package com.local;
//
//import com.local.domain.Parameters;
//import com.local.domain.TimeSeries;
//import com.local.insert.InsertAction;
//import com.local.search.SearchAction;
//import com.local.util.*;
//import com.local.version.VersionAction;
//import javafx.util.Pair;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.util.*;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//class TsToSaxChannel extends ArrayBlockingQueue<Pair<Long, ArrayList<TimeSeries>>> {
//
//    public TsToSaxChannel(int capacity) {
//        super(capacity);
//    }
//
//    public void produce(ArrayList<TimeSeries> tsList, FileEnv fileEnv) {
//
//        System.out.println("生产 " + Thread.currentThread().getName());
//        long offset = InsertAction.putTss(tsList, fileEnv);
//        try {
//            super.put(new Pair<>(offset, tsList));
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }
//    public boolean consume() {
//        System.out.println("消费 " + Thread.currentThread().getName());
//        Pair<Long, ArrayList<TimeSeries>> pair = null;
//        try {
//            pair = super.take();    // 阻塞
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        long offset = pair.getKey();
//        if (offset == -1) { // produce结束
//            return false;
//        }
//        ArrayList<TimeSeries> tsList = pair.getValue();
//        InsertAction.putSaxes(offset, tsList);
//        return true;
//    }
//}
//
//public class Main {
//
//    public static void init() {
//        ArrayList<File> files = null;
//        try {
//
//            files = FileUtil.getAllFile("./db");
//            for (File file: files) {
//                FileUtil.deleteFile(file.getPath());
//            }
//            files = FileUtil.getAllFile("./Ts");
//            for (File file: files) {
//                FileUtil.deleteFile(file.getPath());
//            }
//            int saxtNum = 1000000;
//            FileInputStream input = new FileInputStream("./saxt6.bin"); // 初始化的sax
//            byte[] leaftimekeys = new byte[24*saxtNum];
//            byte[] saxts = new byte[8];
//            for (int i=0;i<saxtNum;i++) {
//                input.read(saxts);
//                System.arraycopy(saxts, 0, leaftimekeys, i*24, 8);
//            }
//
//            CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>()); // 初始化创建worker的时候添加
//            CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
//            VersionAction.initVersion();
//
//            DBUtil.dataBase.open("db");
//            DBUtil.dataBase.init(leaftimekeys, saxtNum);
//
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
//
//    public static Runnable searchThread() {
//        return new Runnable() {
//            @Override
//            public void run() {
//                // 要查的内容
//                byte[] searchTsBytes = new byte[Parameters.timeSeriesDataSize];
//                long startTime = 0;
//                long endTime = new Date().getTime() / 1000;
//                int k = 100;
//
//                System.out.println("开始查询");
//                byte[] ans = SearchAction.searchTs(searchTsBytes, startTime, endTime, k);
//                System.out.println("查询结果 " + Arrays.toString(ans));
//            }
//        };
//    }
//
//    public static Runnable putThread() {
//        return new Runnable() {
//            @Override
//            public void run() {
//                long t1 = System.currentTimeMillis();
//
//                TsToSaxChannel tsToSaxChannel = new TsToSaxChannel(10);
//                int numPutSaxThread = 5;
//                FileChannelReader reader = null;  // 初始化的ts
//                try {
//                    reader = new FileChannelReader(Parameters.FileSetting.inputPath, Parameters.FileSetting.readSize);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                CacheUtil.fileEnv = new FileEnv(new int[]{0});
//
//
//                for (int i = 0; i < numPutSaxThread; i ++ ) {
//                    newFixedThreadPool.execute(new Runnable() {
//                        @Override
//                        public void run() {
//                            while (tsToSaxChannel.consume()) ;
//                        }
//                    });
//                }
//
//                while (true) {   // 从文件读ts
//                    ArrayList<TimeSeries> tsList = InsertAction.readTs(reader);
//                    if (tsList.isEmpty()) {
//                        break;
//                    }
//                    tsToSaxChannel.produce(tsList, CacheUtil.fileEnv);
////            newFixedThreadPool.execute(new Runnable() {
////                @Override
////                public void run() {
////                    tsToSaxChannel.produce(tsList, CacheUtil.fileEnv);
////                }
////            });
//                }
//
//
//                fileTime += System.currentTimeMillis() - t1;
////                printFileTime();
//            }
//        };
//    }
//
//
////    private static void printFileTime() {
////        File file = new File(Parameters.tsFolder + "/" + "Ts0.bin");
////        RandomAccessFile randomAccessFile = null;//r: 只读模式 rw:读写模式
////        try {
////            randomAccessFile = new RandomAccessFile(file, "rw");
////            long pos = randomAccessFile.length();
////            System.out.println("put完成 读时间 " + readTime + " 写时间 " + writeTime + " 读写时间 " + fileTime + " 文件长度" + pos);
////            file = new File(Parameters.FileSetting.inputPath);
////            randomAccessFile = new RandomAccessFile(file, "rw");//r: 只读模式 rw:读写模式
////            System.out.println("文件长度 " + randomAccessFile.length() / 1024 * 1032);
////        } catch (IOException e) {
////            throw new RuntimeException(e);
////        }
////    }
//
//    public static ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(Parameters.numThread);
//    public static long readTime = 0;
//    public static long writeTime = 0;
//    public static long fileTime = 0;
//    public static void main(String[] args) throws IOException, InterruptedException {
//
//
//        init();
//
//        newFixedThreadPool.execute(putThread());
//        newFixedThreadPool.execute(searchThread());
//
//        Thread.sleep(Long.MAX_VALUE);
//        DBUtil.dataBase.close();
//        newFixedThreadPool.shutdown();
//    }
//
//
//}
