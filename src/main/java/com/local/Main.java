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
        ByteBuffer leafTimeKeyBuffer = ByteBuffer.allocateDirect(initTsNum * Parameters.leafTimeKeysSize);
        if (initTsNum <= 2000000) {
            ByteBuffer initTsBuffer = ByteBuffer.allocateDirect(initTsNum * Parameters.tsSize);
            for (int i = 0; i < Parameters.initNum; i ++ ) {
                long offset = reader.getOffset();
                ByteBuffer tsBuffer = reader.read();
                initTsBuffer.put(tsBuffer);
                tsBuffer.flip();
                System.out.println("读文件: " + reader.getFileNum() + " offset:" + offset + " 用于初始化");
            }
            DBUtil.dataBase.init_buffer(initTsNum, initTsBuffer, leafTimeKeyBuffer, 0, 0);
        }
        else if (initTsNum <= 4000000) {
            ByteBuffer initTsBuffer = ByteBuffer.allocateDirect(2000000 * Parameters.tsSize);
            ByteBuffer initTsBuffer1 = ByteBuffer.allocateDirect((initTsNum - 2000000) * Parameters.tsSize);
            for (int i = 0; i < Parameters.initNum; i ++ ) {

                long offset = reader.getOffset();
                ByteBuffer tsBuffer = reader.read();
//                System.out.println(i + " " + tsBuffer.capacity() + " " + i * Parameters.FileSetting.readTsNum );
                if (i * Parameters.FileSetting.readTsNum < 2000000) {
                    initTsBuffer.put(tsBuffer);
                }
                else {
                    initTsBuffer1.put(tsBuffer);
                }
                tsBuffer.flip();
                System.out.println("读文件: " + reader.getFileNum() + " offset:" + offset + " 用于初始化");
            }
            DBUtil.dataBase.init_buffer1(2000000, initTsBuffer, initTsNum - 2000000, initTsBuffer1, leafTimeKeyBuffer, 0, 0);
        }
        else {
            throw new RuntimeException("init大小超过限制");
        }
        PrintUtil.print("初始化成功==========================");
    }

    public static boolean hasInsert = false;
    public static boolean isRecord = true;


    public static long searchTime = 0;
    public static long cntP = 0;
    public static long cntRes = 0;
    public static long totalReadTime = 0;
    public static long totalReadLockTime = 0;
    public static double oneDis = 0;
    public static double totalRecall = 0;
    public static double totalError = 0;

    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner scan = new Scanner(System.in);
        System.out.println("Please enter:   0: Approximate query    1: Accurate query");
//        int isExact = scan.nextInt();
        int isExact = 0;
        System.out.println("Number of queries: ");
//        int queryNum = scan.nextInt();
        int queryNum = 10;
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
         // Search after insert
        Insert insert = new Insert(queryNum, searchLock);

         // Search while insert
        int eachSearchNum = 10; // 一轮查询有几个查询
//        int interval = 10;    // 读几次进行一轮查询
//        int readLimit = 500;   // 读多少次停止
//        int searchStart = 100;  // 读多少次开始查询,确保大于initNum

//        // 确保(readLimit - searchStart) / interval * eachSearchNum >= queryNum
//        if ((readLimit - searchStart) / interval * eachSearchNum < queryNum) throw new RuntimeException("请减少queryNum或interval");

//        Insert2 insert = new Insert2(queryNum, searchLock, readLimit, searchStart, interval, eachSearchNum);
        CacheUtil.insertThreadPool.execute(insert);

        /**
         * Search
         */
        long totalCntP = 0, totalCntRes = 0, totalSearchTime = 0;
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
            System.out.println("查询" + eachSearchNum);
//            Thread.sleep(1000);
            // 运行查询
            for (int i = 0; i < eachSearchNum; i ++ ) {
                cntP = 0;   cntRes = 0;
                search.run();
//                System.out.println("查询时间：" + searchTime);
//                System.out.println("访问原始时间序列个数：" + cntP + "\t返回原始时间序列个数：" + cntRes + "\t读取原始时间序列总时间：" + totalReadTime + "\t平均距离" + oneDis);
//                System.out.println("-----------------------------------------------");
                totalCntP += cntP;  totalCntRes += cntRes;  totalDis += oneDis; totalSearchTime += searchTime;
                if ((i + 1) % 1000 == 0) {
                    System.out.println(i + 1);
                }
            }
            totalSearchNum += eachSearchNum;
            System.out.println("查询完" + totalSearchNum);

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
