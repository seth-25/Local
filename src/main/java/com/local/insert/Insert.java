package com.local.insert;

import com.local.Main;
import com.local.util.CacheUtil;
import com.local.util.MappedFileReader;
import com.local.util.PrintUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class Insert implements Runnable{

    private static int cntRead = 0;
    private static int cntInsert = 0;
    public static long insertTime;
    public static long IOTime = 0;
    public static long CPUTime = 0;
    /**
     * 读文件和发送sax并行
     */
    static class TsToSaxChannel extends ArrayBlockingQueue<TsReadBatch> {
        public TsToSaxChannel(int capacity) {
            super(capacity);
        }
        public void produce() throws InterruptedException {
            boolean flag = true;
            while(flag) {
                for (Map.Entry<Integer, MappedFileReader> entry: CacheUtil.mappedFileReaderMap.entrySet()) {
                    flag = false;
                    // 从文件读ts
                        long IOTimeStart = System.currentTimeMillis();
                    MappedFileReader reader = entry.getValue();
                    long offset = reader.read();
                    byte[] tsBytes = reader.getArray();
                        IOTime += System.currentTimeMillis() - IOTimeStart;
                    if (offset != -1) { // 这个文件没读完
                        flag = true;
                    }
                    else {  // 读完了跳过
                        continue;
                    }
                    System.out.println("读文件: " + reader.getFileNum() + " 次数：" + ++cntRead + " offset:" + offset);

                    TsReadBatch tsReadBatch = new TsReadBatch(tsBytes, reader.getFileNum(), offset);
                    super.put(tsReadBatch);
                }
            }
            super.put(new TsReadBatch(null, -1, -1)); // 结束
        }
        public boolean consume() throws InterruptedException {
            TsReadBatch tsReadBatch = super.take();    // 阻塞
                long CPUTimeStart = System.currentTimeMillis();
            if (tsReadBatch.getFileNum() == -1) {
                System.out.println("读完所有文件,退出\n");
                Main.hasInsert = true;
                System.out.println("插入总时间: " + (System.currentTimeMillis() - insertTime) + "\tIO时间：" + IOTime + "\tCPU时间：" + CPUTime);
                return false;
            }
            System.out.println("插入次数：" + ++cntInsert);
            byte[] leafTimeKeys = InsertAction.getLeafTimeKeysBytes(tsReadBatch.getTsBytes(), tsReadBatch.getFileNum(), tsReadBatch.getOffset());
            InsertAction.putLeafTimeKeysBytes(leafTimeKeys);
                CPUTime += System.currentTimeMillis() - CPUTimeStart;
            return true;
        }
    }
    @Override
    public void run() {
        insertTime = System.currentTimeMillis();
        TsToSaxChannel tsToSaxChannel = new TsToSaxChannel(10);
        CacheUtil.insertThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        if (!tsToSaxChannel.consume()) break;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        PrintUtil.print("开始插入======================");
        try {
            tsToSaxChannel.produce();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
