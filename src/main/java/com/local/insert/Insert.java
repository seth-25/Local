package com.local.insert;

import com.local.Main;
import com.local.util.CacheUtil;
import com.local.util.MappedFileReader;
import com.local.util.PrintUtil;

import java.util.Map;

public class Insert implements Runnable{

    private static int cntRead = 0;
    private static int cntInsert = 0;
    public static long insertTime;
    public static long IOTime = 0;
    public static long CPUTime = 0;
    /**
     * 读文件和发送sax并行
     */
    static class TsToSaxChannel {
        private final int capacity;
        private int cnt;
        private TsReadBatch buffer;
        public TsToSaxChannel(int capacity) {
            this.capacity = capacity;
            this.cnt = 0;
        }
        private synchronized void put(TsReadBatch tsReadBatch) throws InterruptedException {
            while (cnt >= capacity) {
                wait();
            }
            buffer = tsReadBatch;
            cnt ++;
            notifyAll();
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
                    if (offset != -1) { // 这个文件没读完
                        flag = true;
                    }
                    else {  // 读完了跳过
                        continue;
                    }
                    byte[] tsBytes = reader.getArray();
                    TsReadBatch tsReadBatch = new TsReadBatch(tsBytes, reader.getFileNum(), offset);
                    IOTime += System.currentTimeMillis() - IOTimeStart;

                    put(tsReadBatch);
                    System.out.println("读文件: " + reader.getFileNum() + " offset:" + offset );
//                  ++cntRead
//                    if (cntRead == 500) {   // todo todo
//                        put(new TsReadBatch(null, -1, -1)); // 结束
//                        return ;
//                    }
                }
            }
            put(new TsReadBatch(null, -1, -1)); // 结束
        }
        public boolean consume() throws InterruptedException {
            TsReadBatch tsReadBatch;
            synchronized (this) {
                while(cnt <= 0) {
                    wait();
                }
                tsReadBatch = buffer;
            }
            long CPUTimeStart = System.currentTimeMillis();
            if (tsReadBatch.getFileNum() == -1) {
                System.out.println("读完所有文件,退出\n");
                Main.hasInsert = true;
                System.out.println("插入总时间: " + (System.currentTimeMillis() - insertTime) + "\tIO时间：" + IOTime + "\tCPU时间：" + CPUTime);
                return false;
            }
            System.out.println("插入次数：" + ++cntInsert);
//            for (int i = 0; i < 100; i ++ ) {
//                System.out.print(tsReadBatch.getTsBytes()[i] + " ");
//            }
//            System.out.println();

            byte[] leafTimeKeys = InsertAction.getLeafTimeKeysBytes(tsReadBatch.getTsBytes(), tsReadBatch.getFileNum(), tsReadBatch.getOffset());
            InsertAction.putLeafTimeKeysBytes(leafTimeKeys);
            CPUTime += System.currentTimeMillis() - CPUTimeStart;

            synchronized (this) {
                cnt --;
                notifyAll();
            }
            return true;
        }
    }
    @Override
    public void run() {
        insertTime = System.currentTimeMillis();
        TsToSaxChannel tsToSaxChannel = new TsToSaxChannel(1);
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
