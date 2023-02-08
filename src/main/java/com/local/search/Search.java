package com.local.search;

import com.local.Main;
import com.local.domain.Parameters;
import com.local.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

public class Search{
    private final boolean isExact;
    private final byte[] startTimeBytes;
    private final byte[] endTimeBytes;
    private long startTime;
    private long endTime;
    private final byte[] searchTsBytes;

    private final int k;
    private int offset = 0;
    RandomAccessFile randomAccessFile;
    public Search(boolean isExact, int k) {
        this.isExact = isExact;
        this.k = k;
        try {
            randomAccessFile = new RandomAccessFile(Parameters.FileSetting.queryFilePath, "r");//r: 只读模式 rw:读写模式
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        searchTsBytes = new byte[Parameters.timeSeriesDataSize];
        startTimeBytes = new byte[Parameters.timeStampSize];
        endTimeBytes = new byte[Parameters.timeStampSize];
    }
    public void getQuery() {
        try {
            if (Parameters.hasTimeStamp > 0) {  // 有时间戳
                // 查询由1个ts和2个时间戳组成
                randomAccessFile.seek((long) offset * (Parameters.timeStampSize + 2 * Parameters.timeStampSize));
                randomAccessFile.read(searchTsBytes);

                randomAccessFile.seek((long) offset * (Parameters.timeStampSize + 2 * Parameters.timeStampSize) + Parameters.timeSeriesDataSize);
                randomAccessFile.read(startTimeBytes);
                startTime = TsUtil.bytesToLong(startTimeBytes);

                randomAccessFile.seek((long) offset * (Parameters.timeStampSize + 2 * Parameters.timeStampSize) + Parameters.timeSeriesDataSize + Parameters.timeStampSize);
                randomAccessFile.read(endTimeBytes);
                endTime = TsUtil.bytesToLong(endTimeBytes);
            }
            else {
                // 查询只有ts
                randomAccessFile.seek((long) offset * Parameters.timeStampSize);
                randomAccessFile.read(searchTsBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        PrintUtil.print("offset " + offset);
        PrintUtil.print(Arrays.toString(searchTsBytes));
        offset ++ ;
    }
    public void run() {
        getQuery();

        PrintUtil.print("开始查询==========================");
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