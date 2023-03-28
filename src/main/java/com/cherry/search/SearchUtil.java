package com.cherry.search;

import com.cherry.Parameters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

    public static byte[] floatToBytes(float f) {
        int tf = Float.floatToIntBits(f);
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i ++ ) {
            bytes[i] = (byte) (tf >> (i * 8));  // 小端,低到高截断
        }
        return bytes;
    }
    public static float bytesToFloat(byte[] b) {
        int tb = 0;
        tb |= (b[0] & 0xff);
        tb |= (b[1] & 0xff) << 8;
        tb |= (b[2] & 0xff) << 16;
        tb |= (b[3] & 0xff) << 24;
        return Float.intBitsToFloat(tb);
    }
//    public static float computeDist(byte[] a, byte[] b) {
//        assert a.length == b.length;
//        float dis = 0;
//        float[] af = new float[a.length / 4];
//        float[] bf = new float[b.length / 4];
//
//        for (int i = 0; i < a.length; i += 4) {
//            int ta = 0, tb = 0;
//            ta |= (a[i] & 0xff);
//            ta |= (a[i + 1] & 0xff) << 8;
//            ta |= (a[i + 2] & 0xff) << 16;
//            ta |= (a[i + 3] & 0xff) << 24;
//            af[i / 4] = Float.intBitsToFloat(ta);
//            tb |= (b[i] & 0xff);
//            tb |= (b[i + 1] & 0xff) << 8;
//            tb |= (b[i + 2] & 0xff) << 16;
//            tb |= (b[i + 3] & 0xff) << 24;
//            bf[i / 4] = Float.intBitsToFloat(tb);
//        }
//        for (int i = 0; i < af.length; i ++ ) {
//            dis += (af[i] - bf[i]) * (af[i] - bf[i]);
//        }
//        return dis;
//    }

    // aquery(有时间戳): ts 256*4, startTime 8, endTime 8, k 4，paa 4*paa大小, saxt 8/16, 空4位(因为time是long,需对齐)
    public static ByteBuffer makeAQuery(ByteBuffer ts, long startTime, long endTime, int k, ByteBuffer paa, ByteBuffer saxBuffer) {
        ByteBuffer aQuery = ByteBuffer.allocateDirect(Parameters.tsDataSize + 2 * Parameters.timeStampSize +
                8 + 4 * Parameters.paaNum + Parameters.saxTSize).order(ByteOrder.LITTLE_ENDIAN);
        aQuery.put(ts); ts.rewind();
        aQuery.putLong(startTime);
        aQuery.putLong(endTime);
        aQuery.putInt(k);
        aQuery.put(paa);
        aQuery.put(saxBuffer);
        aQuery.rewind();
        return aQuery;
    }
    // aquery(没时间戳): ts 256*4, k 4，paa 4*paa大小, saxt 8/16
    public static ByteBuffer makeAQuery(ByteBuffer ts, int k, ByteBuffer paa, ByteBuffer saxBuffer) {
        ByteBuffer aQuery = ByteBuffer.allocateDirect(Parameters.tsDataSize + 4 +
                4 * Parameters.paaNum + Parameters.saxTSize).order(ByteOrder.LITTLE_ENDIAN);

        aQuery.put(ts); ts.rewind();
        aQuery.putInt(k);
        aQuery.put(paa);
        aQuery.put(saxBuffer);
        aQuery.rewind();
        return aQuery;
    }


    // aquery(有时间戳): ts 256*4, startTime 8, endTime 8, k 4，paa 4*paa大小, saxt 8/16, 空4位(因为time是long,需对齐)
    public static byte[] makeAQuery(byte[] ts, long startTime, long endTime, int k, float[] paa, byte[] saxData) {
        byte[] aQuery = new byte[Parameters.tsDataSize + 2 * Parameters.timeStampSize + 8 + 4 * Parameters.paaNum + Parameters.saxTSize];
        System.arraycopy(ts, 0, aQuery, 0, Parameters.tsDataSize);
        System.arraycopy(longToBytes(startTime), 0, aQuery, Parameters.tsDataSize, Parameters.timeStampSize);
        System.arraycopy(longToBytes(endTime), 0, aQuery, Parameters.tsDataSize + 8, Parameters.timeStampSize);
        System.arraycopy(intToBytes(k), 0, aQuery, Parameters.tsDataSize + 16, 4);
        for (int i = 0; i < Parameters.paaNum; i ++ ) {
            System.arraycopy(floatToBytes(paa[i]), 0, aQuery, Parameters.tsDataSize + 20 + 4 * i, 4);
        }
        System.arraycopy(saxData, 0, aQuery, Parameters.tsDataSize + 20 + 4 * Parameters.paaNum, Parameters.saxTSize);
        return aQuery;
    }

    // aquery(没时间戳): ts 256*4, k 4，paa 4*paa大小, saxt 8/16
    public static byte[] makeAQuery(byte[] ts, int k, float[] paa, byte[] saxData) {
        byte[] aQuery = new byte[Parameters.tsDataSize + 4 + 4 * Parameters.paaNum + Parameters.saxTSize];
        System.arraycopy(ts, 0, aQuery, 0, Parameters.tsDataSize);
        System.arraycopy(intToBytes(k), 0, aQuery, Parameters.tsDataSize, 4);
        for (int i = 0; i < Parameters.paaNum; i ++ ) {
            System.arraycopy(floatToBytes(paa[i]), 0, aQuery, Parameters.tsDataSize + 4 + 4 * i, 4);
        }
        System.arraycopy(saxData, 0, aQuery, Parameters.tsDataSize + 4 + 4 * Parameters.paaNum, Parameters.saxTSize);
        return aQuery;
    }


    static public class SearchContent {
        public byte[] timeSeriesData = new byte[Parameters.tsDataSize];
        public ByteBuffer tsBuffer = ByteBuffer.allocateDirect(Parameters.tsDataSize);
        public long startTime;
        public long endTime;
        public int k;
        public byte[] heap = new byte[8];
        public int needNum;
        public float topDist;
        long[] pArray;
    }

    // info: ts 256*4，starttime 8， endtime 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void analysisInfo(byte[] info, SearchContent aQuery) {
        byte[] intBytes = new byte[4];
        byte[] longBytes = new byte[8];
        System.arraycopy(info, 0, aQuery.timeSeriesData, 0, Parameters.tsDataSize);
        System.arraycopy(info, Parameters.tsDataSize, longBytes, 0, 8);
        aQuery.startTime = bytesToLong(longBytes);
        System.arraycopy(info, Parameters.tsDataSize + 8, longBytes, 0, 8);
        aQuery.endTime = bytesToLong(longBytes);
        System.arraycopy(info, Parameters.tsDataSize + 8 + 8, intBytes, 0, 4);
        aQuery.k = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.tsDataSize + 8 + 8 + 4, intBytes, 0, 4);
        aQuery.needNum = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.tsDataSize + 8 + 8 + 4 + 4, intBytes, 0, 4);
        aQuery.topDist = bytesToFloat(intBytes);
        System.arraycopy(info, Parameters.tsDataSize + 8 + 8 + 4 + 4 + 4, intBytes, 0, 4);
        int numSearch = bytesToInt(intBytes);
        aQuery.pArray = new long[numSearch];
        for (int i = 0; i < numSearch; i ++ ) {
            System.arraycopy(info, Parameters.tsDataSize + 8 + 8 + 4 + 4 + 4 + 4 + 8 * i, longBytes, 0, 8);
            long p = bytesToLong(longBytes);
            aQuery.pArray[i] = p;
        }
    }
    // info: ts 256*4， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p*n 8*n
    public static void analysisInfoNoTime(byte[] info, SearchContent aQuery) {
        byte[] intBytes = new byte[4];
        System.arraycopy(info, 0, aQuery.timeSeriesData, 0, Parameters.tsDataSize);
        System.arraycopy(info, Parameters.tsDataSize, intBytes, 0, 4);
        aQuery.k = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.tsDataSize + 4, intBytes, 0, 4);
        aQuery.needNum = bytesToInt(intBytes);
        System.arraycopy(info, Parameters.tsDataSize + 4 + 4, intBytes, 0, 4);
        aQuery.topDist = bytesToFloat(intBytes);
        System.arraycopy(info, Parameters.tsDataSize + 4 + 4 + 4, intBytes, 0, 4);
        int numSearch = bytesToInt(intBytes);
        aQuery.pArray = new long[numSearch];
        for (int i = 0; i < numSearch; i ++ ) {
            byte[] longBytes = new byte[8];
            System.arraycopy(info, Parameters.tsDataSize + 4 + 4 + 4 + 4 + 8 * i, longBytes, 0, 8);
            long p = bytesToLong(longBytes);
            aQuery.pArray[i] = p;
        }
    }

    // info(有时间戳): ts 256*4，starttime 8， endtime 8, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void analysisInfoHeap(ByteBuffer info, SearchContent aQuery) {
        info.limit(Parameters.tsDataSize);
        aQuery.tsBuffer.clear();
        aQuery.tsBuffer.put(info);
        info.limit(info.capacity());

        aQuery.startTime = info.getLong();
        aQuery.endTime = info.getLong();
        info.get(aQuery.heap);
        aQuery.k = info.getInt();
        aQuery.needNum = info.getInt();
        aQuery.topDist = info.getFloat();
        int numSearch = info.getInt();
        aQuery.pArray = new long[numSearch];
        info.asLongBuffer().get(aQuery.pArray);
        info.rewind();
//        System.out.println(" " + info);
    }

    // info(没时间戳): ts 256*4, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void analysisInfoNoTimeHeap(ByteBuffer info, SearchContent aQuery) {
//        System.out.println(info);
        info.limit(Parameters.tsDataSize);
        aQuery.tsBuffer.clear();
        aQuery.tsBuffer.put(info);
        info.limit(info.capacity());
        info.get(aQuery.heap);
        aQuery.k = info.getInt();
        aQuery.needNum = info.getInt();
        aQuery.topDist = info.getFloat();
        int numSearch = info.getInt();
        aQuery.pArray = new long[numSearch];
        info.asLongBuffer().get(aQuery.pArray);
        info.rewind();
//        System.out.println(" " + info);
    }

}
