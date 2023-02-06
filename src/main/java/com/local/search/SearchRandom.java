package com.local.search;

import com.local.Main;
import com.local.domain.Parameters;
import com.local.util.CacheUtil;
import com.local.util.MappedFileReader;

import java.util.Date;
import java.util.Random;

public class SearchRandom implements Runnable{
    private final boolean isExact;
    private long startTime;
    private long endTime;
    private byte[] searchTsBytes;
    private final int k;
    public SearchRandom(boolean isExact, int k) {
        this.isExact = isExact;
        this.k = k;
    }
    public void getQuery() {
        // 生成查询
        Random random = new Random();
        MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(random.nextInt(CacheUtil.mappedFileReaderMap.size()));
        int tsOffset = random.nextInt((int) (reader.getFileLength() / Parameters.tsSize));

        synchronized (reader) {
            searchTsBytes = reader.readTsNewByte(tsOffset);  // 查找文件中的随机一个
        }
        startTime = random.nextLong() % (new Date().getTime() / 1000);
        endTime = random.nextLong() % (new Date().getTime() / 1000);
        if (startTime < 0) startTime = - startTime;
        if (endTime < 0) endTime = - endTime;
        if (startTime > endTime) {
            long tmp = startTime;
            startTime = endTime;
            endTime = tmp;
        }
    }
    @Override
    public void run() {
        getQuery();

        System.out.println("开始查询");
        if (isExact) {
            byte[] ans = SearchAction.searchExactTs(searchTsBytes, startTime, endTime, k);
        }
        else {
            // 返回若干个ares,ares的最后有一个4字节的id
            // ares(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐), p 8
            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), p 8
            byte[] ans = SearchAction.searchTs(searchTsBytes, startTime, endTime, k);
            double dis = 0;
            for (int i = 0; i < ans.length - 4; i += Parameters.aresSize) {
                byte[] floatBytes = new byte[4];
                System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
                dis += Math.sqrt(SearchUtil.bytesToFloat(floatBytes));
            }
            Main.totalDis += dis / k;
        }
    }
}