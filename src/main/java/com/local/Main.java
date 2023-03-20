package com.local;

import com.local.domain.Parameters;
import com.local.insert.Insert;
import com.local.insert.Insert2;
import com.local.search.SearchBuffer;
import com.local.util.*;
import com.local.version.VersionAction;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;


public class Main {
    public static void init() {
        CacheUtil.clearCache.run(); // 清除缓存

        FileUtil.createFolder("./db");
        FileUtil.deleteFolderFiles("./db");

        MappedFileReaderBuffer reader = CacheUtil.mappedFileReaderMapBuffer.get(0);
        if (reader == null) {
            throw new RuntimeException("ts文件夹下没有文件");
        }

        CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>()); // 初始化创建worker的时候添加
        CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
        VersionAction.initVersion();

        DBUtil.dataBase.open("db");
        int initTsNum = Parameters.FileSetting.readTsNum * Parameters.initNum;
//        ByteBuffer leafTimeKeyBuffer = ByteBuffer.allocateDirect(initTsNum * Parameters.leafTimeKeysSize);

        DBUtil.dataBase.init_malloc(initTsNum);
        for (int i = 0; i < Parameters.initNum; i ++ ) {
            long offset = reader.getOffset();
            ByteBuffer tsBuffer = reader.read();
            tsBuffer.flip();
            if (offset % 1000000 == 0) {
                System.out.println("读文件: " + reader.getFileNum() + " offset:" + offset + " 用于初始化");
            }
            DBUtil.dataBase.init_putbuffer(i*Parameters.FileSetting.readTsNum, Parameters.FileSetting.readTsNum, tsBuffer, 0, offset);
        }
        System.out.println("初始化插入次数：" + Parameters.initNum);
        DBUtil.dataBase.init_finish(initTsNum);
        PrintUtil.print("初始化成功==========================");
    }

    public static boolean hasInsert = false;
    public static boolean isRecord = true;


    public static long searchTime = 0;
    public static long cntP = 0;
    public static long cntRes = 0;
    public static long readTime = 0;
    public static long totalReadLockTime = 0;
    public static double searchDis = 0;
    public static double totalRecall = 0;
    public static double totalError = 0;

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner scan = new Scanner(System.in);
        System.out.println("Please enter:   0: Approximate query    1: Accurate query");
//        int isExact = scan.nextInt();
        int isExact = 1;
        System.out.println("Number of queries: ");
//        int queryNum = scan.nextInt();
        int queryNum = 100;
        System.out.println("k: ");
//        int k = scan.nextInt();
        int k = 100;

        /**
         * init
         */
        FileUtil.createFolder(Parameters.FileSetting.inputPath);
        ArrayList<File> files = FileUtil.getAllFile(Parameters.FileSetting.inputPath);
        int fileNum = -1;
        for (File file: files) {
            MappedFileReaderBuffer reader = new MappedFileReaderBuffer(file.getPath(), Parameters.FileSetting.readSize, ++ fileNum );  // 初始化的ts
            CacheUtil.mappedFileReaderMapBuffer.put(fileNum, reader);
        }
        init();

        /**
         * Insert
         */
        SearchLock searchLock = new SearchLock();
        int eachSearchNum = 1; // 一轮查询有几个查询
         // Search after insert
//        Insert insert = new Insert(queryNum, searchLock);
//
//         // Search while insert
        int interval = 40;    // 读几次进行一轮查询
        int searchStart = 1000;  // 读多少次开始查询,确保大于initNum

        // 确保(readLimit - searchStart) / interval * eachSearchNum >= queryNum

        Insert2 insert = new Insert2(queryNum, searchLock, searchStart, interval, eachSearchNum);
        CacheUtil.insertThreadPool.execute(insert);

        /**
         * Search
         */
        long totalCntP = 0, totalCntRes = 0, totalSearchTime = 0, totalReadTime = 0;
        double totalDis = 0;

        SearchBuffer search;
        if (isExact == 1) {
            search = new SearchBuffer(true, k);
        }
        else {
            search = new SearchBuffer(false, k);
        }

        int totalSearchNum = 0;

        while(true) {
            searchLock.lock.lock();
            if (searchLock.searchNum == 0) searchLock.condition.await();
            eachSearchNum = searchLock.searchNum;
            searchLock.lock.unlock();
            System.out.println("一轮查询个数：" + eachSearchNum);
//            Thread.sleep(1000);
            // 运行查询
            for (int i = 0; i < eachSearchNum; i ++ ) {
                cntP = 0;   cntRes = 0; readTime = 0;
                search.run();
                System.out.println("访问原始时间序列个数：" + cntP + "\t返回原始时间序列个数：" + cntRes +
                        "\t读取原始时间序列时间：" + readTime + "\t查询时间：" + searchTime + "\t平均距离" + searchDis);
                System.out.println("-----------------------------------------------");
                totalCntP += cntP;  totalCntRes += cntRes;  totalDis += searchDis; totalSearchTime += searchTime; totalReadTime += readTime;
//                if ((i + 1) % 1000 == 0) {
//                    System.out.println("查询次数" + (i + 1));
//                }
                System.out.println("查询次数" + (i + 1));
            }
            totalSearchNum += eachSearchNum;
            System.out.println("总共查询次数" + totalSearchNum);

            searchLock.lock.lock();
            searchLock.searchNum = 0;
            searchLock.condition.signal();
            searchLock.lock.unlock();

            if (totalSearchNum >= queryNum) {
                if (isExact != 1) {
                    System.out.println("召回率：" + (totalRecall / queryNum) + "\t错误率" + (totalError / queryNum));
                }
                System.out.println("查询总时间：" + totalSearchTime + "\t平均时间" + ((double)totalSearchTime /queryNum));
                System.out.println("读取原始时间序列总时间：" + totalReadTime + "\t平均时间" + ((double)totalReadTime/queryNum));
                System.out.println("查询总距离:" + totalDis + "\t平均距离" + (totalDis / queryNum));
                System.out.println("总共/平均返回原始时间序列：" + totalCntRes + "/" + ((double) totalCntRes / queryNum) +
                        "\t总共/平均访问原始时间序列：" + totalCntP + "/" + ((double)totalCntP / queryNum) +
                        "\t返回/访问比例：" + ((double) totalCntRes / totalCntP));
                break;
            }
        }

        ///////////////////////////////////////////////////////////////////////


        Thread.sleep(Long.MAX_VALUE);
        DBUtil.dataBase.close();
        CacheUtil.insertThreadPool.shutdown();
//        CacheUtil.searchThreadPool.shutdown();
    }


}
