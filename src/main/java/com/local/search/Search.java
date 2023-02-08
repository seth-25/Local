package com.local.search;

import com.local.Main;
import com.local.domain.Parameters;
import com.local.util.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class Search implements Runnable{
    private final boolean isExact;
    private final byte[] startTimeBytes;
    private final byte[] endTimeBytes;
    private long startTime;
    private long endTime;
    public static byte[] searchTsBytes = new byte[Parameters.timeSeriesDataSize];

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
    private void getQuery() {
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
    private void computeDis(byte[] ans) {
        double dis = 0;
        for (int i = 0; i < ans.length - 4; i += Parameters.aresSize) {
            byte[] floatBytes = new byte[4];
            System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
            dis += Math.sqrt(SearchUtil.bytesToFloat(floatBytes));
        }
        Main.totalDis += dis / k;
    }

    static class Ts {
        byte[] ts = new byte[Parameters.tsSize];    // 时间序列+时间戳(如果有)
        Float dis;
        public Ts(byte[] ts, float dis) {
            this.ts = ts;
            this.dis = dis;
        }
    }
    private void computeRecallAndError(byte[] ans) {
        ArrayList<Ts> approAnsList = new ArrayList<>();
        for (int i = 0; i < ans.length - 4; i += Parameters.aresSize) {
            byte[] tsBytes = new byte[Parameters.timeSeriesDataSize];
            System.arraycopy(ans, i, tsBytes, 0, Parameters.timeSeriesDataSize);
            byte[] floatBytes = new byte[4];
            System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
            approAnsList.add(new Ts(tsBytes, SearchUtil.bytesToFloat(floatBytes)));
        }

        ArrayList<Ts> exactAnsList = new ArrayList<>();
        byte[] exactAns = SearchAction.searchExactTs(searchTsBytes, startTime, endTime, k);
        for (int i = 0; i < exactAns.length; i += Parameters.aresExactSize) {
            byte[] tsBytes = new byte[Parameters.timeSeriesDataSize];
            System.arraycopy(ans, i, tsBytes, 0, Parameters.timeSeriesDataSize);
            byte[] floatBytes = new byte[4];
            System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
            exactAnsList.add(new Ts(tsBytes, SearchUtil.bytesToFloat(floatBytes)));
        }
        approAnsList.sort(new Comparator<Ts>() {
            @Override
            public int compare(Ts o1, Ts o2) {
                return o1.dis.compareTo(o2.dis);
            }
        });
        exactAnsList.sort(new Comparator<Ts>() {
            @Override
            public int compare(Ts o1, Ts o2) {
                return o1.dis.compareTo(o2.dis);
            }
        });

        assert approAnsList.size() == k;
        assert exactAnsList.size() == k;
        System.out.println(approAnsList.size() + " " + exactAnsList.size());
        int cnt = 0;
        for (Ts approTs: approAnsList) {
            for (Ts exactTs: exactAnsList) {
                if (TsUtil.compareTs(approTs.ts, exactTs.ts)) {
                    cnt ++;
                    break;
                }
            }
        }
        System.out.println("Recall:" + ((double)cnt / k));
        Main.totalRecall += ((double)cnt / k);

        double error = 0;
        for (int i = 0; i < k; i ++ ) {
//            error += approAnsList.get(i).dis / exactAnsList.get(i).dis;
            System.out.println(approAnsList.get(i).dis + " " + exactAnsList.get(i).dis);
        }

        System.out.println("Error:" + (error / k));
        Main.totalError += error;

    }
    @Override
    public void run() {
        getQuery();
        byte[] ans;
        PrintUtil.print("开始查询==========================");

        long searchTimeStart = System.currentTimeMillis();
        if (isExact) {
            ans = SearchAction.searchExactTs(searchTsBytes, startTime, endTime, k);
            Main.searchTime += System.currentTimeMillis() - startTime;
        }
        else {
            // 返回若干个ares,ares的最后有一个4字节的id
            // ares(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐), p 8
            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), p 8
            ans = SearchAction.searchTs(searchTsBytes, startTime, endTime, k);
            Main.searchTime += System.currentTimeMillis() - searchTimeStart;

            computeDis(ans);
            computeRecallAndError(ans);
        }
    }
}