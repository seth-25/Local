package com.cherry.insert;

import com.cherry.Main;
import com.cherry.SearchLock;
import com.cherry.domain.Parameters;
import com.cherry.util.*;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Search after insert
 */
public class Insert implements Runnable{
    private int queryNum;
    private SearchLock searchLock;
    public Insert(int queryNum, SearchLock searchLock) {
        this.queryNum = queryNum;
        this.searchLock = searchLock;
    }

    private static int cntRead = 0;
    private static int cntInsert = Parameters.initNum;
    public static long insertTime;
    public static long IOTime = 0;
    public static long[] CPUTime = new long[Parameters.insertNumThread];

    public static long[] saxtTime = new long[Parameters.insertNumThread];
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
        public void produce() throws InterruptedException {
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
                    if (tsBuffer != null) { // 这个文件没读完
                        flag = true;
                    }
                    else {  // 读完了跳过
                        continue;
                    }
                    TsReadBatch tsReadBatch = new TsReadBatch(tsBuffer, reader.getFileNum(), offset);
                    IOTime += System.currentTimeMillis() - IOTimeStart;

                    put(tsReadBatch);
                    if (offset % 1000000 == 0) {
                        System.out.println("读文件: " + reader.getFileNum() + " offset:" + offset );
                    }

                    ++cntRead;
                    if (cntRead == Parameters.FileSetting.readLimit - Parameters.initNum) {   // 提前结束
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
        public boolean consume(ByteBuffer leafTimeKeysBuffer, int i) throws InterruptedException {
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

            long CPUTimeStart = System.currentTimeMillis();
            saxtTime[i] += DBUtil.dataBase.put_buffer(Parameters.FileSetting.readTsNum, tsReadBatch.getTsBuffer(),
                    leafTimeKeysBuffer, tsReadBatch.getFileNum(), tsReadBatch.getOffset()) / 1000;
            CPUTime[i] += System.currentTimeMillis() - CPUTimeStart;

            synchronized (this) {
                cnt --; // insert完才-1，防止tsBytes被覆盖
//                tsReadBatch.getReader().arraysListOffer(tsReadBatch.getTsBytes());
                notifyAll();
                if (++cntInsert % 10 == 0) {
                    System.out.println("插入次数：" + cntInsert);
                }
            }
            return true;
        }
    }
    @Override
    public void run() {
        insertTime = System.currentTimeMillis();

        // 不同的consumer可能同时结束，除了consumer占用的tsBytes，还要再预留consumer个tsBytes，共2*insertNumThread个
        // 先read再put，wait条件是等于capacity，所以至多同时存在capacity+1个tsBytes，故capacity设成2 * insertNumThread - 1
        TsToSaxChannel tsToSaxChannel = new TsToSaxChannel(2 * Parameters.insertNumThread - 1);
        for (int i = 0; i < Parameters.insertNumThread; i ++ ) {
            int finalI = i;
            CacheUtil.insertThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    ByteBuffer leafTimeKeysBuffer = ByteBuffer.allocateDirect(Parameters.FileSetting.readTsNum * Parameters.leafTimeKeysSize);
                    while(true) {
                        try {
                            if (!tsToSaxChannel.consume(leafTimeKeysBuffer, finalI)) break;
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        }

        PrintUtil.print("开始插入======================");
        try {
            tsToSaxChannel.produce();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        System.out.println("读完所有文件,退出\n");
        Main.hasInsert = true;
        System.out.println("插入总时间: " + (System.currentTimeMillis() - insertTime) + "\tIO时间：" + IOTime + "\tCPU时间：" + Arrays.toString(CPUTime));
        System.out.println("Ts转化成saxT时间：" + Arrays.toString(saxtTime));
        CacheUtil.insertThreadPool.shutdown();

        // todo todo
        Scanner scan = new Scanner(System.in);
        System.out.println("清理缓存:");
        scan.next();
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        DBUtil.dataBase.print_time();




        searchLock.lock.lock();
        try {
            searchLock.searchNum = queryNum;
            searchLock.condition.signal();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            searchLock.lock.unlock();
        }

    }

    public int getCntInsert() {
        return cntInsert;
    }
}
