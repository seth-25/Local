package com.cherry;

import com.cherry.insert.Insert;
import com.cherry.insert.Insert2;
import com.cherry.search.Search;
import com.cherry.util.*;
import com.cherry.version.VersionAction;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;


public class Main {
    public static void init() {
//        CacheUtil.clearCache.run(); // clear cache

        FileUtil.createFolder("./db");
        FileUtil.deleteFolderFiles("./db");

        MappedFileReaderBuffer reader = CacheUtil.mappedFileReaderMapBuffer.get(0);
        if (reader == null) {
            throw new RuntimeException("No files under the ts folder");
        }

        CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>());
        CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
        VersionAction.initVersion();

        DBUtil.dataBase.open("db");
        int initTsNum = Parameters.FileSetting.readTsNum * Parameters.initNum;

        DBUtil.dataBase.init_malloc(initTsNum);
        for (int i = 0; i < Parameters.initNum; i ++ ) {
            long offset = reader.getOffset();
            ByteBuffer tsBuffer = reader.read();
            tsBuffer.flip();
            if (offset % 1000000 == 0) {
                System.out.println("read file:" + reader.getFileNum() + " offset:" + offset + " for init");
            }
            DBUtil.dataBase.init_putbuffer(i*Parameters.FileSetting.readTsNum, Parameters.FileSetting.readTsNum, tsBuffer, 0, offset);
        }
        System.out.println("init number of insertions：" + Parameters.initNum);
        DBUtil.dataBase.init_finish(initTsNum);
        PrintUtil.print("===================== init success =====================");
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
        int isExact = scan.nextInt();
//        int isExact = 1;
        System.out.println("Number of queries: ");
        int queryNum = scan.nextInt();
//        int queryNum = 10;
        System.out.println("k: ");
        int k = scan.nextInt();
//        int k = 100;

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
        FileUtil.checkFileExists(Parameters.FileSetting.queryFilePath);

        /**
         * Insert
         * Default query after inserting all data
         * If you want to insert while searching (update workload), replace insertThreadPool.execute(insert) with insert2
         * And make sure (Parameters.FileSetting.readLimit - searchStart) / interval * eachSearchNum >= queryNum
         */
        SearchLock searchLock = new SearchLock();
        int eachSearchNum = 10; // A round of searching has several queries

        // Search after inserting
        Insert insert = new Insert(queryNum, searchLock);
        CacheUtil.insertThreadPool.execute(insert);

        // Search while inserting
        int interval = 400;    // How many times to insert between two rounds of search
        int searchStart = 1000;  // How many times to read before starting search, make sure searchStart > Parameters.initNum
//        Insert2 insert2 = new Insert2(queryNum, searchLock, searchStart, interval, eachSearchNum);
//        CacheUtil.insertThreadPool.execute(insert2);

        /**
         * Search
         */
        long totalCntP = 0, totalCntRes = 0, totalSearchTime = 0, totalReadTime = 0;
        double totalDis = 0;

        Search search;
        if (isExact == 1) {
            search = new Search(true, k);
        }
        else {
            search = new Search(false, k);
        }

        int totalSearchNum = 0;

        while(true) {
            searchLock.lock.lock();
            if (searchLock.searchNum == 0) searchLock.condition.await();
            eachSearchNum = searchLock.searchNum;
            searchLock.lock.unlock();
            System.out.println("Number of queries included in a search round: " + eachSearchNum);
            // Run search
            for (int i = 0; i < eachSearchNum; i ++ ) {
                cntP = 0;   cntRes = 0; readTime = 0;
                search.run();
                System.out.println("Number of accesses to raw data:" + cntP + "\tNumber of returns of raw data:" + cntRes +
                        "\tTime of reading raw data:" + readTime + "\tTime of querying:" + searchTime + "\tAverage Distance:" + searchDis);
//                if ((i + 1) % 1000 == 0) {
//                    System.out.println("Number of queries" + (i + 1));
//                }
                System.out.println("Number of queries:" + (i + 1));
                System.out.println("-----------------------------------------------");
                totalCntP += cntP;  totalCntRes += cntRes;  totalDis += searchDis; totalSearchTime += searchTime; totalReadTime += readTime;

            }
            totalSearchNum += eachSearchNum;
            System.out.println("Total number of queries:" + totalSearchNum);

            searchLock.lock.lock();
            searchLock.searchNum = 0;
            searchLock.condition.signal();
            searchLock.lock.unlock();
            if (totalSearchNum >= queryNum) {
//                if (isExact != 1) {
                    // need to uncomment computeRecallAndError in Search.class
//                    System.out.println("Recall rate：" + (totalRecall / queryNum) + "\terror rate" + (totalError / queryNum));
//                }
                System.out.println("Total time of querying: " + totalSearchTime + "\tAverage Time" + ((double)totalSearchTime /queryNum));
                System.out.println("Total time of reading raw data: " + totalReadTime + "\tAverage Time" + ((double)totalReadTime/queryNum));
                System.out.println("Total Distance: :" + totalDis + "\tAverage Distance: " + (totalDis / queryNum));
                System.out.println("Total/Average number of accesses to raw data:" + totalCntP + "/" + ((double)totalCntP / queryNum) +
                        "\tTotal/Average number of returns of raw data:" + totalCntRes + "/" + ((double) totalCntRes / queryNum) +
                        "\treturn/access rate：" + ((double) totalCntRes / totalCntP));


//                System.out.println("Total time of update workload:" + (System.currentTimeMillis() - Insert2.insertTimeStart));
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
