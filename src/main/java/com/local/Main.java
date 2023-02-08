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
    public static long cntP ;
    public static long totalCntRes = 0;
    public static long totalReadTime = 0;
    public static long totalReadLockTime = 0;
    public static double totalDis = 0;
    public static long approCnt = 0;
    public static void main(String[] args) throws IOException, InterruptedException {
        Scanner scan = new Scanner(System.in);
        System.out.println("Please enter:   0: Approximate query    1: Accurate query");
        int isExact = scan.nextInt();
        System.out.println("Number of queries: ");
        int queryNum = scan.nextInt();

        FileUtil.createFolder(Parameters.FileSetting.inputPath);
        ArrayList<File> files = FileUtil.getAllFile(Parameters.FileSetting.inputPath);
        int fileNum = -1;
        for (File file: files) {
            MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, ++ fileNum );  // 初始化的ts
            CacheUtil.mappedFileReaderMap.put(fileNum, reader);
        }

        init();

        Thread.sleep(5000);

//        insertTime = System.currentTimeMillis();
        CacheUtil.insertThreadPool.execute(new Insert());
        long maxCntP = 0;
        long totalCntP = 0;

        while(true) {
//            if (CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName) != null) {    // 等到初始化得到版本
            if (hasInsert) {    // 插入完成后再查询
                Thread.sleep(10000);

                if (isExact == 1) {
                    Search exactSearch = new Search(true, 100);
                    long startTime = System.currentTimeMillis();
                    for (int i = 0; i < queryNum; i ++ ) {
                        long startQuery = System.currentTimeMillis();
                        cntP = 0;
                        exactSearch.run();

                        System.out.println("精确查询时间：" + (System.currentTimeMillis() - startQuery));
                        System.out.println("访问原始时间序列个数：" + cntP);
                        System.out.println("-----------------------------------------------");
                        maxCntP = Math.max(maxCntP, cntP);
                        totalCntP += cntP;
                    }
                    System.out.println("查询总时间：" + (System.currentTimeMillis() - startTime));
                    System.out.println("读取原始时间序列总时间：" + totalReadTime + " " + totalReadLockTime);
                    System.out.println("最大访问原始时间序列：" + maxCntP);
                    System.out.println("总共访问原始时间序列：" + totalCntP + " 总共返回原始时间序列：" + totalCntRes + " 比例：" + ((double)totalCntRes / totalCntP));
                }
                else {
                    Search approximateSearch = new Search(false, 100);
                    long startTime = System.currentTimeMillis();
                    for (int i = 0; i < queryNum; i ++) {
                        cntP = 0;
                        long startQuery = System.currentTimeMillis();
                        approximateSearch.run();
                        System.out.println("近似查询时间:" + (System.currentTimeMillis() - startQuery));
                        System.out.println("访问原始时间序列个数：" + cntP);
                        System.out.println("-----------------------------------------------");
                        maxCntP = Math.max(maxCntP, cntP);
                        totalCntP += cntP;
                    }

                    System.out.println("查询总时间：" + (System.currentTimeMillis() - startTime));
                    System.out.println("读取原始时间序列总时间：" + totalReadTime + " " + totalReadLockTime);
                    System.out.println("近似距离:" + totalDis + " 平均距离" + totalDis / queryNum);
                    System.out.println("最大访问原始时间序列：" + maxCntP);
                    System.out.println("总共访问原始时间序列：" + totalCntP + " 总共返回原始时间序列：" + totalCntRes + " 比例：" + ((double)totalCntRes / totalCntP));
                }




                break;
            }

            Thread.sleep(100);
        }
        ///////////////////////////////////////////////////////////////////////


        Thread.sleep(Long.MAX_VALUE);
        DBUtil.dataBase.close();
        CacheUtil.insertThreadPool.shutdown();
    }


}
