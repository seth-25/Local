package com.local.insert;


import com.local.Main;
import com.local.SearchLock;
import com.local.domain.Parameters;
import com.local.util.*;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Search while insert
 */
public class Insert2 implements Runnable{
    private int queryNum;
    private int readLimit;
    private int searchStart;
    private int interval;
    private int eachSearchNum;
    SearchLock searchLock;
    public Insert2(int queryNum, SearchLock searchLock, int readLimit, int searchStart, int interval, int eachSearchNum) {
        this.queryNum = queryNum;
        this.searchLock = searchLock;
        this.readLimit = readLimit;
        this.searchStart = searchStart;
        this.interval = interval;
        this.eachSearchNum = eachSearchNum;
    }

    private static int cntRead = 0;
    private static int cntInsert = 0;
    public static long insertTime;
    public static long IOTime = 0;
    /**
     * 读文件和发送sax并行
     */
    static class TsToSaxChannel {
        private final int capacity;
        private int cnt, cntGet;
        private final TsReadBatch[] buffer;
        private int head, tail;
        public TsToSaxChannel(int capacity) {
            this.capacity = capacity;
            this.cnt = 0;
            this.cntGet = 0;
            this.buffer = new TsReadBatch[capacity];
        }
        private synchronized void put(TsReadBatch tsReadBatch) throws InterruptedException {
            while (cnt >= capacity) {
                wait();
            }
            buffer[tail] = tsReadBatch;
            tail = (tail + 1) % capacity;
            cnt ++ ;
            cntGet ++ ;
            notifyAll();
        }
        public void produce(int readLimit) throws InterruptedException {
            boolean flag = true;
            while(flag) {
                for (Map.Entry<Integer, MappedFileReaderBuffer> entry: CacheUtil.mappedFileReaderMapBuffer.entrySet()) {
//                for (Map.Entry<Integer, FileChannelReader> entry: CacheUtil.fileChannelReaderMap.entrySet()) {
                    flag = false;

                    // 从文件读ts
                    long IOTimeStart = System.currentTimeMillis();
                    MappedFileReaderBuffer reader = entry.getValue();
                    long offset = reader.getOffset();
                    ByteBuffer tsBuffer = reader.read();
//                    ByteBuffer tsBuffer = ByteBuffer.allocateDirect(1);
                    if (tsBuffer != null) { // 这个文件没读完
                        flag = true;
                    }
                    else {  // 读完了跳过
                        continue;
                    }
                    TsReadBatch tsReadBatch = new TsReadBatch(tsBuffer, reader.getFileNum(), offset);
                    IOTime += System.currentTimeMillis() - IOTimeStart;


                    put(tsReadBatch);
                    System.out.println("读文件: " + reader.getFileNum() + " offset:" + offset );

                    ++cntRead;

                    if (cntRead == readLimit - Parameters.initNum) {   // 提前结束
                        for (int i = 0; i < Parameters.insertNumThread; i ++ ) {
                            put(new TsReadBatch(null, -1, -1)); // 结束标识
                        }
                        return ;
                    }
                }
            }
            for (int i = 0; i < Parameters.insertNumThread; i ++ ) {
                put(new TsReadBatch(null, -1, -1)); // 结束consume
            }

        }
        public boolean consume(ByteBuffer leafTimeKeysBuffer, SearchLock searchLock, int searchStart, int interval, int eachSearchNum) throws InterruptedException {
            TsReadBatch tsReadBatch;
            synchronized (this) {
                while(cntGet <= 0) {
                    wait();
                }
                tsReadBatch = buffer[head];
                head = (head + 1) % capacity;
                cntGet -- ; // 获取完tsReadBatch就-1，防止不同consume获取同个tsReadBatch
            }

            if (tsReadBatch.getFileNum() == -1) {
                return false;
            }


//            byte[] tsBytes = new byte[Parameters.FileSetting.readSize];
//            tsReadBatch.getTsBuffer().get(tsBytes);
//            tsReadBatch.getTsBuffer().rewind();
//            for (int i = 0; i < 100; i ++ ) {
//                System.out.print(tsBytes[i] + " ");
//            }
//            System.out.println();

            DBUtil.dataBase.put_buffer(Parameters.FileSetting.readTsNum, tsReadBatch.getTsBuffer(), leafTimeKeysBuffer, tsReadBatch.getFileNum(), tsReadBatch.getOffset());

            synchronized (this) {
                cnt --; // insert完才-1，防止tsBytes被覆盖
                notifyAll();
                System.out.println("插入次数：" + ++cntInsert);

                if (cntInsert + Parameters.initNum > searchStart && (cntInsert + Parameters.initNum) % interval == 0) {
                    searchLock.lock.lock();
                    if (searchLock.searchNum > 0) searchLock.condition.await();
                    searchLock.searchNum = eachSearchNum;
                    searchLock.condition.signal();
                    searchLock.lock.unlock();
//                    try {
//                        searchLock.searchNum ++;
//                        totalSearchNum ++;
//                        System.out.println("查询次数 " + totalSearchNum + " 插入次数：" + cntInsert + " 查询队列个数" + searchLock.searchNum);
//                        searchLock.condition.signal();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    } finally {
//                        searchLock.lock.unlock();
//                    }
                }
            }
            return true;
        }
    }
    static int totalSearchNum = 0;
    @Override
    public void run() {
        insertTime = System.currentTimeMillis();

        // 不同的consumer可能同时结束，除了consumer占用的tsBytes，还要再预留consumer个tsBytes，共2*insertNumThread个
        // 先read再put，wait条件是等于capacity，所以至多同时存在capacity+1个tsBytes，故capacity设成2 * insertNumThread - 1
        TsToSaxChannel tsToSaxChannel = new TsToSaxChannel(2 * Parameters.insertNumThread - 1);
        for (int i = 0; i < Parameters.insertNumThread; i ++ ) {
            CacheUtil.insertThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    ByteBuffer leafTimeKeysBuffer = ByteBuffer.allocateDirect(Parameters.FileSetting.readTsNum * Parameters.leafTimeKeysSize);
                    while(true) {
                        try {
                            if (!tsToSaxChannel.consume(leafTimeKeysBuffer, searchLock, searchStart, interval, eachSearchNum)) break;
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        }

        PrintUtil.print("开始插入======================");
        try {
            tsToSaxChannel.produce(readLimit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        System.out.println("读完所有文件,退出\n");
        Main.hasInsert = true;
        System.out.println("插入总时间: " + (System.currentTimeMillis() - insertTime) + "\tIO时间：" + IOTime);
        CacheUtil.insertThreadPool.shutdown();

    }

    public int getCntInsert() {
        return cntInsert;
    }
}
