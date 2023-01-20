package com.local.insert;

import com.local.util.CacheUtil;
import com.local.util.MappedFileReader;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class Insert implements Runnable{

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
//                        long t = System.currentTimeMillis();
                    MappedFileReader reader = entry.getValue();
                    long offset = reader.read();
                    byte[] tsBytes = reader.getArray();
//                        readTime += System.currentTimeMillis() - t;
                    if (offset != -1) { // 这个文件没读完
                        flag = true;
                    }
                    else {  // 读完了跳过
                        continue;
                    }
                    System.out.println("读文件: " + reader.getFileNum());

                    TsReadBatch tsReadBatch = new TsReadBatch(tsBytes, reader.getFileNum(), offset);
                    super.put(tsReadBatch);

                }
            }
            super.put(new TsReadBatch(null, -1, -1)); // 结束
        }
        public boolean consume() throws InterruptedException {
            TsReadBatch tsReadBatch = super.take();    // 阻塞
//                System.out.println("消费 " + cnt + " " + Thread.currentThread().getName());
            if (tsReadBatch.getFileNum() == -1) {
                System.out.println("读完所有文件,退出");
                return false;
            }
            byte[] leafTimeKeys = InsertAction.getLeafTimeKeysBytes(tsReadBatch.getTsBytes(), tsReadBatch.getFileNum(), tsReadBatch.getOffset());
            InsertAction.putSaxesBytes(leafTimeKeys);
            return true;
        }
    }
    @Override
    public void run() {
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

        try {
            tsToSaxChannel.produce();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
