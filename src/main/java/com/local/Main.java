package com.local;

import com.local.domain.Parameters;
import com.local.insert.Insert;
import com.local.insert.InsertAction;
import com.local.search.Search;
import com.local.search.SearchAction;
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
            throw new RuntimeException("ts文件夹下没有文件, reader不存在");
        }
        long offset = reader.read();
        byte[] tsBytes = reader.getArray();
        System.out.println("ts长度" + tsBytes.length);
        byte[] leafTimeKeysBytes = InsertAction.getLeafTimeKeysBytes(tsBytes, reader.getFileNum(), offset, true);
        System.out.println("leafTimeKeys长度" + leafTimeKeysBytes.length);

        CacheUtil.workerInVerRef.put(Parameters.hostName, new HashMap<>()); // 初始化创建worker的时候添加
        CacheUtil.workerOutVerRef.put(Parameters.hostName, new HashMap<>());
        VersionAction.initVersion();

        DBUtil.dataBase.open("db");
        DBUtil.dataBase.init(leafTimeKeysBytes, saxtNum);
        System.out.println("初始化成功==========================");

    }



    public static long insertTime = 0;
    public static boolean hasInsert = false;


    public static void main(String[] args) throws IOException, InterruptedException {

        ArrayList<File> files = FileUtil.getAllFile(Parameters.FileSetting.inputPath);
        int fileNum = -1;
        for (File file: files) {
            MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, ++ fileNum );  // 初始化的ts
            CacheUtil.mappedFileReaderMap.put(fileNum, reader);
        }

        init();

        Thread.sleep(3000);

        insertTime = System.currentTimeMillis();
        CacheUtil.insertThreadPool.execute(new Insert());

        while(true) {
//            if (CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName) != null) {    // 等到初始化得到版本
            if (hasInsert) {    // 等到初始化得到版本
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < 100; i ++ ) {
                    new Search(true, 100).run();
                    System.out.println("-----------------------------------------------\n");
                    System.out.println("精确查询时间" + (System.currentTimeMillis() - startTime));
                }
                System.out.println("精确查询总时间" + (System.currentTimeMillis() - startTime));
//                for (int i = 0; i < 100000; i ++) {
////                    CacheUtil.searchThreadPool.execute(new Search(false, 100));
//                    new Search(false, 100).run();
//                    System.out.println("查询完成");
//                }
                break;
            }

            Thread.sleep(100);
        }
        ///////////////////////////////////////////////////////////////////////


        Thread.sleep(Long.MAX_VALUE);
        DBUtil.dataBase.close();
        CacheUtil.insertThreadPool.shutdown();
        CacheUtil.searchThreadPool.shutdown();
    }


}
