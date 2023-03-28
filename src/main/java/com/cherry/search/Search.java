package com.cherry.search;

import com.cherry.Main;
import com.cherry.Parameters;
import com.cherry.util.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;

public class Search implements Runnable{
    private final boolean isExact;
    private final byte[] startTimeBytes;
    private final byte[] endTimeBytes;
    private long startTime;
    private long endTime;
    private final ByteBuffer searchTsBuffer;
    private final ByteBuffer startTimeBuffer;
    private final ByteBuffer endTimeBuffer;
    private final int k;
    private int offset = 0;
    RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    public Search(boolean isExact, int k) {
        this.isExact = isExact;
        this.k = k;
        try {
            fileChannel = new FileInputStream(Parameters.FileSetting.queryFilePath).getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.startTimeBytes = new byte[Parameters.timeStampSize];
        this.endTimeBytes = new byte[Parameters.timeStampSize];
        this.searchTsBuffer = ByteBuffer.allocateDirect(Parameters.tsDataSize);
        this.startTimeBuffer = ByteBuffer.allocate(Parameters.timeStampSize).order(ByteOrder.LITTLE_ENDIAN);
        this.endTimeBuffer = ByteBuffer.allocate(Parameters.timeStampSize).order(ByteOrder.LITTLE_ENDIAN);
    }
    private void getQuery() {
        try {
            if (Parameters.hasTimeStamp > 0) {  // 有时间戳
                // 查询由1个ts和2个时间戳组成
                searchTsBuffer.clear();
                fileChannel.read(searchTsBuffer, (long) offset * (2 * Parameters.timeStampSize + Parameters.tsDataSize));
                searchTsBuffer.rewind();

                startTimeBuffer.clear();
                fileChannel.read(startTimeBuffer, (long) offset * (2 * Parameters.timeStampSize + Parameters.tsDataSize) + Parameters.tsDataSize);
                startTimeBuffer.flip();
                startTime = startTimeBuffer.getLong();


                endTimeBuffer.clear();
                fileChannel.read(endTimeBuffer, (long) offset * (2 * Parameters.timeStampSize + Parameters.tsDataSize) + Parameters.tsDataSize + Parameters.timeStampSize);
                endTimeBuffer.flip();
                endTime = endTimeBuffer.getLong();

            }
            else {
                // 查询只有ts
                searchTsBuffer.clear();
                fileChannel.read(searchTsBuffer, (long) offset * Parameters.tsDataSize);
                searchTsBuffer.rewind();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        PrintUtil.print("offset " + offset);
//        PrintUtil.print(Arrays.toString(searchTsBytes));
        offset ++ ;
    }
    private void computeDis(byte[] ans) {
        double dis = 0;
        PrintUtil.PrintDis printDis = new PrintUtil.PrintDis("ApproximateDis.txt");   // todo
        for (int i = 0; i < ans.length - 4; i += Parameters.approximateResSize) {
            byte[] floatBytes = new byte[4];
            System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
            float d = SearchUtil.bytesToFloat(floatBytes);
            dis += Math.sqrt(d);

            printDis.print(d);  // todo

//            System.out.println(SearchUtil.bytesToFloat(floatBytes));    // todo
        }
        printDis.close(); // todo
        Main.searchDis = dis / k;
    }

    private void computeExactDis(byte[] ans) {
        double dis = 0;
//        double oldDis = 0;
        PrintUtil.PrintDis printDis = new PrintUtil.PrintDis("ExactDis.txt");
        for (int i = 0; i < ans.length; i += Parameters.exactResSize) {
            byte[] floatBytes = new byte[4];
            System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
            float d = SearchUtil.bytesToFloat(floatBytes);
            dis += Math.sqrt(d);

            printDis.print(d);   // todo


//            System.out.println(d);    // todo
//            if (Math.sqrt(d) == oldDis) { // todo
//                System.out.println("重复 " + SearchUtil.bytesToFloat(floatBytes));    // todo
//            }

//            oldDis = Math.sqrt(d);
        }
        printDis.close();    // todo
        Main.searchDis = dis / k;
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
        Main.isRecord = false;  // 计算召回率和错误率时不要记录io时间和访问次数
        ArrayList<Ts> approAnsList = new ArrayList<>();
        for (int i = 0; i < ans.length - 4; i += Parameters.approximateResSize) {
            byte[] tsBytes = new byte[Parameters.tsDataSize];
            System.arraycopy(ans, i, tsBytes, 0, Parameters.tsDataSize);
            byte[] floatBytes = new byte[4];
            System.arraycopy(ans, i + Parameters.tsSize, floatBytes, 0, 4);
            approAnsList.add(new Ts(tsBytes, SearchUtil.bytesToFloat(floatBytes)));

//            byte[] saxTbytes = new byte[Parameters.saxTSize];   // todo
//            DBUtil.dataBase.saxt_from_ts(tsBytes, saxTbytes);   // todo
//            System.out.println("appro:" + Arrays.toString(saxTbytes));  // todo
        }

        ArrayList<Ts> exactAnsList = new ArrayList<>();

        PrintUtil.print("计算回归率，进行精确查询");
        searchTsBuffer.rewind();
        ByteBuffer exactRes = SearchAction.searchExactTs(searchTsBuffer, startTime, endTime, k);
        byte[] exactAns = new byte[exactRes.remaining()];
        exactRes.get(exactAns);
        exactRes.clear();
        for (int i = 0; i < exactAns.length; i += Parameters.exactResSize) {
            byte[] tsBytes = new byte[Parameters.tsDataSize];
            System.arraycopy(exactAns, i, tsBytes, 0, Parameters.tsDataSize);
            byte[] floatBytes = new byte[4];
            System.arraycopy(exactAns, i + Parameters.tsSize, floatBytes, 0, 4);
            exactAnsList.add(new Ts(tsBytes, SearchUtil.bytesToFloat(floatBytes)));

//            byte[] saxTbytes = new byte[Parameters.saxTSize];   // todo
//            DBUtil.dataBase.saxt_from_ts(tsBytes, saxTbytes);   // todo
//            System.out.println("exact:" + Arrays.toString(saxTbytes));  // todo
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

        double exactDis = 0;
        for (int i = 0; i < k; i ++ ) {
            System.out.println("近似距离" + approAnsList.get(i).dis + "\t精确距离" + exactAnsList.get(i).dis);
            exactDis += Math.sqrt(exactAnsList.get(i).dis);
        }
        exactDis /= k;
        System.out.println("精确平均距离" + exactDis);

        for (int i = 0; i < k; i ++ ) {
            if (exactAnsList.get(i).dis == 0) {
                System.out.println("exact query dis = 0, unable to calculate error");
                Main.totalError = Double.NaN;
                Main.isRecord = true;
                return ;
            }
            error += approAnsList.get(i).dis / exactAnsList.get(i).dis;
        }

        System.out.println("Error:" + (error / k));
        Main.totalError += (error / k);

        Main.isRecord = true;
    }
    @Override
    public void run() {
        getQuery();

        PrintUtil.print("开始查询==========================");

        long searchTimeStart = System.currentTimeMillis();
        if (isExact) {
            // ares_exact(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐)
            // ares_exact(有时间戳): ts 256*4, float dist 4
            // Get_exact返回若干个ares_exact, 这个ares_exact没有p也不用空4位
            ByteBuffer exactRes = SearchAction.searchExactTs(searchTsBuffer, startTime, endTime, k);
            Main.searchTime = System.currentTimeMillis() - searchTimeStart;
            byte[] ans = new byte[exactRes.remaining()];
            exactRes.get(ans);
            computeExactDis(ans);
        }
        else {
            // ares(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐), p 8
            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), p 8
            // Get返回若干个ares,ares的最后有一个4字节的id,用于标记近似查的是当前am版本中的哪个表(一个am版本有多个表并行维护不同的saxt树),用于精准查询的appro_res(去重)
            ByteBuffer ares = SearchAction.searchTs(searchTsBuffer, startTime, endTime, k);
            Main.searchTime = System.currentTimeMillis() - searchTimeStart;

            byte[] ans = new byte[ares.remaining()];
            ares.get(ans);
            computeDis(ans);
//            computeRecallAndError(ans);
        }
    }
}