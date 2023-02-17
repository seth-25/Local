package com.local.search;

import com.local.domain.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchUtil {

    public static long bytesToLong(byte[] bytes) {
        long res = 0;
        for (int i = 0; i < 8; i ++ ) {
            res <<= 8;
            res |= (bytes[8 - 1 - i] & 0xff);  // 小端
        }
        return res;
    }

    public static byte[] longToBytes(long l) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i ++ ) {
            bytes[i] = (byte) (l >> (i * 8));   // 小端，截断低位给bytes低地址
        }
        return bytes;
    }

    public static int bytesToInt(byte[] bytes) {
        int res = 0;
        for (int i = 0; i < 4; i ++ ) {
            res <<= 8;
            res |= (bytes[4 - 1 - i] & 0xff);  // 小端
        }
        return res;
    }

    public static byte[] intToBytes(int l) {
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i ++ ) {
            bytes[i] = (byte) (l >> (i * 8));   // 小端，截断低位给bytes低地址
        }
        return bytes;
    }

    static byte[] floatToBytes(float f) {
        int tf = Float.floatToIntBits(f);
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i ++ ) {
            bytes[i] = (byte) (tf >> (i * 8));  // 小端,低到高截断
        }
        return bytes;
    }
    static float bytesToFloat(byte[] b) {
        int tb = 0;
        tb |= (b[0] & 0xff);
        tb |= (b[1] & 0xff) << 8;
        tb |= (b[2] & 0xff) << 16;
        tb |= (b[3] & 0xff) << 24;
        return Float.intBitsToFloat(tb);
    }
    static float computeDist(byte[] a, byte[] b) {
        assert a.length == b.length;
        float dis = 0;
        float[] af = new float[a.length / 4];
        float[] bf = new float[b.length / 4];

        for (int i = 0; i < a.length; i += 4) {
            int ta = 0, tb = 0;
            ta |= (a[i] & 0xff);
            ta |= (a[i + 1] & 0xff) << 8;
            ta |= (a[i + 2] & 0xff) << 16;
            ta |= (a[i + 3] & 0xff) << 24;
            af[i / 4] = Float.intBitsToFloat(ta);
            tb |= (b[i] & 0xff);
            tb |= (b[i + 1] & 0xff) << 8;
            tb |= (b[i + 2] & 0xff) << 16;
            tb |= (b[i + 3] & 0xff) << 24;
            bf[i / 4] = Float.intBitsToFloat(tb);
        }
        for (int i = 0; i < af.length; i ++ ) {
            dis += (af[i] - bf[i]) * (af[i] - bf[i]);
        }
        return dis;
    }

    // aquery(有时间戳): ts 256*4, startTime 8, endTime 8, k 4，paa 4*paa大小, saxt 8/16, 空4位(因为time是long,需对齐)
    public static byte[] makeAQuery(byte[] ts, long startTime, long endTime, int k, float[] paa, byte[] saxData) {
        byte[] aQuery = new byte[Parameters.timeSeriesDataSize + 2 * Parameters.timeStampSize + 8 + 4 * Parameters.paaNum + Parameters.saxTSize];
        System.arraycopy(ts, 0, aQuery, 0, Parameters.timeSeriesDataSize);
        System.arraycopy(longToBytes(startTime), 0, aQuery, Parameters.timeSeriesDataSize, Parameters.timeStampSize);
        System.arraycopy(longToBytes(endTime), 0, aQuery, Parameters.timeSeriesDataSize + 8, Parameters.timeStampSize);
        System.arraycopy(intToBytes(k), 0, aQuery, Parameters.timeSeriesDataSize + 16, 4);
        for (int i = 0; i < Parameters.paaNum; i ++ ) {
            System.arraycopy(floatToBytes(paa[i]), 0, aQuery, Parameters.timeSeriesDataSize + 20 + 4 * i, 4);
        }
        System.arraycopy(saxData, 0, aQuery, Parameters.timeSeriesDataSize + 20 + 4 * Parameters.paaNum, Parameters.saxTSize);
        return aQuery;
    }

    // aquery(没时间戳): ts 256*4, k 4，paa 4*paa大小, saxt 8/16
    public static byte[] makeAQuery(byte[] ts, int k, float[] paa, byte[] saxData) {
        byte[] aQuery = new byte[Parameters.timeSeriesDataSize + 4 + 4 * Parameters.paaNum + Parameters.saxTSize];
        System.arraycopy(ts, 0, aQuery, 0, Parameters.timeSeriesDataSize);
        System.arraycopy(intToBytes(k), 0, aQuery, Parameters.timeSeriesDataSize, 4);
        for (int i = 0; i < Parameters.paaNum; i ++ ) {
            System.arraycopy(floatToBytes(paa[i]), 0, aQuery, Parameters.timeSeriesDataSize + 4 + 4 * i, 4);
        }
        System.arraycopy(saxData, 0, aQuery, Parameters.timeSeriesDataSize + 4 + 4 * Parameters.paaNum, Parameters.saxTSize);
        return aQuery;
    }


    static public class SearchContent {
        public byte[] timeSeriesData = new byte[Parameters.timeSeriesDataSize];
        public long startTime;
        public long endTime;
        public int k;
        public byte[] heap = new byte[8];
        public int needNum;
        public float topDist;
        List<Long> pList = new ArrayList<>();
        /**
         * 排序，同一个文件挨着搜
         */
        public void sortPList() {
            Collections.sort(pList);
        }
    }

    // info: ts 256*4，starttime 8， endtime 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void analysisSearchSend(byte[] info, SearchContent aQuery) {
        byte[] intBytes = new byte[4];
        byte[] longBytes = new byte[8];
        System.arraycopy(info, 0, aQuery.timeSeriesData, 0, Parameters.timeSeriesDataSize);
        System.arraycopy(info, Parameters.timeSeriesDataSize, longBytes, 0, 8);
        aQuery.startTime = bytesToLong(longBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 8, longBytes, 0, 8);
        aQuery.endTime = bytesToLong(longBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8, intBytes, 0, 4);
        aQuery.k = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 4, intBytes, 0, 4);
        aQuery.needNum = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 4 + 4, intBytes, 0, 4);
        aQuery.topDist = bytesToFloat(intBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 4 + 4 + 4, intBytes, 0, 4);
        int numSearch = bytesToInt(intBytes);
        for (int i = 0; i < numSearch; i ++ ) {
            System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 4 + 4 + 4 + 4 + 8 * i, longBytes, 0, 8);
            Long p = bytesToLong(longBytes);
            aQuery.pList.add(p);
        }
    }
    // info: ts 256*4， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p*n 8*n
    public static void analysisSearchSendNoTime(byte[] info, SearchContent aQuery) {
        byte[] intBytes = new byte[4];
        System.arraycopy(info, 0, aQuery.timeSeriesData, 0, Parameters.timeSeriesDataSize);
        System.arraycopy(info, Parameters.timeSeriesDataSize, intBytes, 0, 4);
        aQuery.k = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 4, intBytes, 0, 4);
        aQuery.needNum = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize+ 4 + 4, intBytes, 0, 4);
        aQuery.topDist = bytesToFloat(intBytes);
        System.arraycopy(info, Parameters.timeSeriesDataSize + 4 + 4 + 4, intBytes, 0, 4);
        int numSearch = bytesToInt(intBytes);
        for (int i = 0; i < numSearch; i ++ ) {
            byte[] longBytes = new byte[8];
            System.arraycopy(info, Parameters.timeSeriesDataSize + 4 + 4 + 4 + 4 + 8 * i, longBytes, 0, 8);
            Long p = bytesToLong(longBytes);
            aQuery.pList.add(p);
        }
    }

    // info(有时间戳): ts 256*4，starttime 8， endtime 8, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void analysisSearchSendHeap(byte[] info, SearchContent aQuery) {
        byte[] intBytes = new byte[4];
        byte[] longBytes = new byte[8];
        System.arraycopy(info, 0, aQuery.timeSeriesData, 0, Parameters.timeSeriesDataSize);

        System.arraycopy(info, Parameters.timeSeriesDataSize, longBytes, 0, 8);
        aQuery.startTime = bytesToLong(longBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8, longBytes, 0, 8);
        aQuery.endTime = bytesToLong(longBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8, aQuery.heap, 0, 8);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 8, intBytes, 0, 4);
        aQuery.k = bytesToInt(intBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 8 + 4, intBytes, 0, 4);
        aQuery.needNum = bytesToInt(intBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 8 + 4 + 4, intBytes, 0, 4);
        aQuery.topDist = bytesToFloat(intBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 8 + 4 + 4 + 4, intBytes, 0, 4);

        int numSearch = bytesToInt(intBytes);
        for (int i = 0; i < numSearch; i ++ ) {
            System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 8 * i, longBytes, 0, 8);
            Long p = bytesToLong(longBytes);
            aQuery.pList.add(p);
        }
    }
    // info(没时间戳): ts 256*4, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void analysisSearchSendNoTimeHeap(byte[] info, SearchContent aQuery) {
        byte[] intBytes = new byte[4], longBytes = new byte[8];;
        System.arraycopy(info, 0, aQuery.timeSeriesData, 0, Parameters.timeSeriesDataSize);

        System.arraycopy(info, Parameters.timeSeriesDataSize, aQuery.heap, 0, 8);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8, intBytes, 0, 4);
        aQuery.k = bytesToInt(intBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 4, intBytes, 0, 4);
        aQuery.needNum = bytesToInt(intBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 4 + 4, intBytes, 0, 4);
        aQuery.topDist = bytesToFloat(intBytes);

        System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 4 + 4 + 4, intBytes, 0, 4);
        int numSearch = bytesToInt(intBytes);

        for (int i = 0; i < numSearch; i ++ ) {
            System.arraycopy(info, Parameters.timeSeriesDataSize + 8 + 4 + 4 + 4 + 4 + 8 * i, longBytes, 0, 8);
            Long p = bytesToLong(longBytes);
            aQuery.pList.add(p);
        }
    }

}
