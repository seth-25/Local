package com.local.search;

import com.local.domain.Parameters;
import com.local.util.CacheUtil;
import com.local.util.MappedFileReader;

import java.util.Date;
import java.util.Random;

public class Search implements Runnable{
    private final boolean isExact;
    private long startTime;
    private long endTime;
    private byte[] searchTsBytes;
    private final int k;
    public Search(boolean isExact, int k) {
        this.isExact = isExact;
        this.k = k;
    }
    public void getQuery() {
        // 生成查询
        Random random = new Random();
        MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(random.nextInt(CacheUtil.mappedFileReaderMap.size()));
        int tsOffset = random.nextInt((int) (reader.getFileLength() / Parameters.tsSize));

        synchronized (reader) {
            searchTsBytes = reader.readTs(tsOffset);  // 查找文件中的随机一个
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
            byte[] ans = SearchAction.searchTs(searchTsBytes, startTime, endTime, k);
        }
    }
}