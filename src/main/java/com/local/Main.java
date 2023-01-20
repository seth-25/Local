package com.local;

import com.local.domain.Parameters;
import com.local.insert.InsertAction;
import com.local.search.SearchAction;
import com.local.util.*;
import com.local.version.VersionAction;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class Main {
    public static void init1() {
        ArrayList<File> files = null;
        try {

            files = FileUtil.getAllFile("./db");
            for (File file: files) {
                FileUtil.deleteFile(file.getPath());
            }
            int saxtNum = 1000000;

            byte[] leaftimekeys = new byte[saxtNum * Parameters.saxSize];
            boolean flag = true;
            int i = 0;
            while(flag) {
                for (Map.Entry<Integer, MappedFileReader> entry: CacheUtil.mappedFileReaderMap.entrySet()) {
                    long readStartTime = System.currentTimeMillis();
                    flag = false;
                    // 从文件读ts
//                        long t = System.currentTimeMillis();
                    MappedFileReader reader = entry.getValue();
                    long offset = reader.read();
                    byte[] tsBytes = reader.getArray();
//                        readTime += System.currentTimeMillis() - t;
                    if (offset != -1) { // 这个文件没读完
                        flag = true;
                    }
                    else {  // 读完了跳过
                        continue;
                    }
                    readTime += System.currentTimeMillis() - readStartTime;
                    System.out.println("读" + (++cnt));
//                        ArrayList<Sax> saxes = InsertAction.getSaxes(tsBytes, reader.getFileNum(), offset);
//                        InsertAction.putSaxes(saxes);
                    byte[] leafTimeKeysBytes = InsertAction.getLeafTimeKeysBytes(tsBytes, reader.getFileNum(), offset);
                    System.arraycopy(leafTimeKeysBytes, 0, leaftimekeys, i * 100000 * Parameters.saxSize, 100000 * Parameters.saxSize);
                    i++;
                }
            }

            //排序
            DBUtil.dataBase.leaftimekey_sort(leaftimekeys);

            CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>()); // 初始化创建worker的时候添加
            CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
            VersionAction.initVersion();

            DBUtil.dataBase.open("db");
            DBUtil.dataBase.init(leaftimekeys, saxtNum);
            System.out.println("初始化成功==========================");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void init() {
        ArrayList<File> files = null;
        try {
            files = FileUtil.getAllFile("./db");
            for (File file: files) {
                FileUtil.deleteFile(file.getPath());
            }
            int saxtNum = Parameters.FileSetting.readTsNum;

            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(0);
            if (reader == null) {
                throw new RuntimeException("reader不存在");
            }
            long offset = reader.read();
            byte[] tsBytes = reader.getArray();
            byte[] leafTimeKeysBytes = InsertAction.getLeafTimeKeysBytes(tsBytes, reader.getFileNum(), offset, true);
            System.out.println(leafTimeKeysBytes.length);

            CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>()); // 初始化创建worker的时候添加
            CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
            VersionAction.initVersion();

            DBUtil.dataBase.open("db");
            DBUtil.dataBase.init(leafTimeKeysBytes, saxtNum);
            System.out.println("初始化成功==========================");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static Runnable searchThread() {
        return new Runnable() {
            @Override
            public void run() {

                FileChannelReader reader2 = null;
                try {
                    reader2 = new FileChannelReader(Parameters.FileSetting.inputPath + "output.bin", 1024, 0 );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                reader2.read();
                byte[] searchTsBytes = reader2.getArray();  // 查插入的第一个

                // 要查的内容
//                byte[] searchTsBytes = new byte[Parameters.timeSeriesDataSize];
                long startTime = 0;
                long endTime = new Date().getTime() / 1000;
                int k = 100;

                System.out.println("开始查询");
                byte[] ans = SearchAction.searchTs(searchTsBytes, startTime, endTime, k);
                System.out.println("查询结果 " + Arrays.toString(ans));

            }
        };
    }

    static int cnt = 0;
    public static Runnable putThread() {
        return new Runnable() {
            @Override
            public void run() {
                boolean flag = true;
                while(flag) {
                    for (Map.Entry<Integer, MappedFileReader> entry: CacheUtil.mappedFileReaderMap.entrySet()) {
                        flag = false;
                        // 从文件读ts
                        MappedFileReader reader = entry.getValue();
                        long offset = reader.read();
                        byte[] tsBytes = reader.getArray();
                        if (offset != -1) { // 这个文件没读完
                            flag = true;
                        }
                        else {  // 读完了跳过
                            continue;
                        }
                        System.out.println("读" + (++cnt));
//                        ArrayList<Sax> saxes = InsertAction.getSaxes(tsBytes, reader.getFileNum(), offset);
//                        InsertAction.putSaxes(saxes);
                        byte[] leafTimeKeysBytes = InsertAction.getLeafTimeKeysBytes(tsBytes, reader.getFileNum(), offset);
                        InsertAction.putSaxesBytes(leafTimeKeysBytes);
                    }
                }
            }
        };
    }


    public static ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(Parameters.numThread);

    public static long readTime = 0;
    public static long writeTime = 0;
    public static long fileTime = 0;


    public static void main(String[] args) throws IOException, InterruptedException {

        ArrayList<File> files = FileUtil.getAllFile(Parameters.FileSetting.inputPath);
        int fileNum = -1;
        for (File file: files) {
            MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, ++ fileNum );  // 初始化的ts
            CacheUtil.mappedFileReaderMap.put(fileNum, reader);
        }

        init1();




//        System.out.println("查询结果 " + Arrays.toString(ans));


//        while(true) {
//            if (CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName) != null) {    // 等到初始化得到版本
//                Thread.sleep(3000);
//                for (int i = 0; i < 10; i ++) {
//                    newFixedThreadPool.execute(searchThread());
//                }
//                break;
//            }
//            Thread.sleep(100);
//        }


        /////////////////////////////////////////////////////////////////////////


        Thread.sleep(Long.MAX_VALUE);
        DBUtil.dataBase.close();
        newFixedThreadPool.shutdown();
    }


}
