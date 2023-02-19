package com.local;

import com.local.domain.Parameters;
import com.local.insert.Insert;
import com.local.insert.InsertAction;
import com.local.search.Search;
import com.local.util.*;
import com.local.version.VersionAction;

import java.io.*;
import java.util.*;



public class Main {
    public static void init() {
        FileUtil.createFolder("./db");
        FileUtil.deleteFolderFiles("./db");
        int saxtNum = Parameters.FileSetting.readTsNum;

        MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(0);
        if (reader == null) {
            throw new RuntimeException("ts文件夹下没有文件");
        }
        long offset = reader.read();
        byte[] tsBytes = reader.getArray();
        System.out.println("读文件: " + reader.getFileNum() + " 次数：" + 0 + " offset:" + offset + " 用于初始化");
        PrintUtil.print("ts长度" + tsBytes.length);


        byte[] leafTimeKeysBytes = InsertAction.getLeafTimeKeysBytes(tsBytes, reader.getFileNum(), offset, true);
        PrintUtil.print("leafTimeKeys长度" + leafTimeKeysBytes.length);

        CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>()); // 初始化创建worker的时候添加
        CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
        VersionAction.initVersion();

        DBUtil.dataBase.open("db");
        DBUtil.dataBase.init(leafTimeKeysBytes, saxtNum);
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
        int isExact = scan.nextInt();
//        int isExact = 1;
        System.out.println("Number of queries: ");
        int queryNum = scan.nextInt();
//        int queryNum = 1;

        FileUtil.createFolder(Parameters.FileSetting.inputPath);
        ArrayList<File> files = FileUtil.getAllFile(Parameters.FileSetting.inputPath);
        int fileNum = -1;
        for (File file: files) {
            MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, ++ fileNum );  // 初始化的ts
            CacheUtil.mappedFileReaderMap.put(fileNum, reader);
        }

        init();

        Thread.sleep(5000);

        /**
         * Insert
         */
        CacheUtil.insertThreadPool.execute(new Insert());

        /**
         * Search
         */
        long totalCntP = 0, totalCntRes = 0;
        double totalDis = 0;
        while(true) {
//            if (CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName) != null) {    // 等到初始化得到版本
            if (hasInsert) {    // 插入完成后再查询
                Thread.sleep(10000);

                if (isExact == 1) {
                    Search exactSearch = new Search(true, 100);
                    for (int i = 0; i < queryNum; i ++ ) {
                        cntP = 0;   cntRes = 0;
                        long startQuery = System.currentTimeMillis();
                        exactSearch.run();

                        System.out.println("精确查询时间：" + (System.currentTimeMillis() - startQuery));
                        System.out.println("访问原始时间序列个数：" + cntP + "\t返回原始时间序列个数：" + cntRes + "\t读取原始时间序列总时间：" + totalReadTime + "\t平均距离" + oneDis);
                        System.out.println("-----------------------------------------------");
                        totalCntP += cntP;  totalCntRes += cntRes;  totalDis += oneDis;
                    }
                    System.out.println("查询总时间：" + Main.searchTime + "\t读取原始时间序列总时间：" + totalReadTime);
                    System.out.println("精确查询总距离:" + totalDis + "\t平均距离" + (totalDis / queryNum));
                    System.out.println("总共/平均访问原始时间序列：" + totalCntP + "/" + ((double)totalCntP / queryNum) +
                            "\t总共/平均返回原始时间序列：" + totalCntRes + "/" + ((double) totalCntRes / queryNum) +
                            "\t返回/访问比例：" + ((double) totalCntRes / totalCntP));
                }
                else {
                    Search approximateSearch = new Search(false, 100);
                    for (int i = 0; i < queryNum; i ++) {
                        cntP = 0;   cntRes = 0;
                        long startQuery = System.currentTimeMillis();
                        approximateSearch.run();
                        System.out.println("近似查询时间:" + (System.currentTimeMillis() - startQuery));
                        System.out.println("访问原始时间序列个数：" + cntP + "\t返回原始时间序列个数：" + cntRes + "\t读取原始时间序列总时间：" + totalReadTime + "\t平均距离" + oneDis);
                        System.out.println("-----------------------------------------------");
                        totalCntP += cntP; totalCntRes += cntRes;   totalDis += oneDis;
                    }
                    System.out.println("查询总时间：" + Main.searchTime + "\t读取原始时间序列总时间：" + totalReadTime);
                    System.out.println("近似查询总距离:" + totalDis + "\t平均距离" + (totalDis / queryNum));
                    System.out.println("总共/平均访问原始时间序列：" + totalCntP + "/" + ((double)totalCntP / queryNum) +
                            "\t总共/平均返回原始时间序列：" + totalCntRes + "/" + ((double) totalCntRes / queryNum) +
                            "\t返回/访问比例：" + ((double) totalCntRes / totalCntP));
                    System.out.println("召回率：" + (totalRecall / queryNum) + "\t错误率" + (totalError / queryNum));
                }
                break;
            }

            Thread.sleep(100);
        }
        ///////////////////////////////////////////////////////////////////////


        Thread.sleep(Long.MAX_VALUE);
        DBUtil.dataBase.close();
        CacheUtil.insertThreadPool.shutdown();
//        CacheUtil.searchThreadPool.shutdown();
    }


}
