package com.local.version;

import com.local.domain.Parameters;

import java.util.ArrayList;
import java.util.Arrays;

public class VersionUtil {

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

    public static byte[] intToBytes(long l) {
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i ++ ) {
            bytes[i] = (byte) (l >> (i * 8));   // 小端，截断低位给bytes低地址
        }
        return bytes;
    }

    public static double saxToDouble(byte[] saxBytes) {
        long l = 0;
        for (int i = saxBytes.length - 1; i >= 0; i --) {
            l <<= 8;
            l |= (saxBytes[i] & 0xff);
        }
        // 翻转首位
        if ((saxBytes[saxBytes.length - 1] & 0x80) == 0) {
            l |= 0x8000000000000000L;
        }
        else {
            l &= 0x7fffffffffffffffL;
        }

        return (double) l;
    }

    //第一个字节为0， 发送versionid 4字节, amV_id 4字节, number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节，
    public static void analysisVersionBytes(byte[] versionBytes,
                                            Integer[] inVer, Integer[] outVer, Long[] fileNum,
                                            byte[] minSax, byte[] maxSax, Long[] minTime, Long[] maxTime) {
        assert inVer.length == 1 && outVer.length == 1 && fileNum.length == 1 && minTime.length == 1 && maxTime.length == 1;
        assert minSax.length == Parameters.saxDataSize && maxSax.length == Parameters.saxDataSize;

        byte[] intBytes = new byte[4], longBytes = new byte[8];
        System.arraycopy(versionBytes, 1, intBytes, 0, 4);
        outVer[0] = bytesToInt(intBytes);
        System.arraycopy(versionBytes, 1 + 4, intBytes, 0, 4);
        inVer[0] = bytesToInt(intBytes);
        System.arraycopy(versionBytes, 1 + 4 + 4, longBytes, 0, 8);
        fileNum[0] = bytesToLong(longBytes);
        System.arraycopy(versionBytes, 1 + 4 + 4 + 8, minSax, 0, 8);
        System.arraycopy(versionBytes, 1 + 4 + 4 + 8 + 8, maxSax, 0, 8);
        System.arraycopy(versionBytes, 1 + 4 + 4 + 8 + 8 + 8, longBytes, 0, 8);
        minTime[0] = bytesToLong(longBytes);
        System.arraycopy(versionBytes, 1 + 4 + 4 + 8 + 8 + 8 + 8, longBytes, 0, 8);
        maxTime[0] = bytesToLong(longBytes);
    }

    //第一个字节为1，发送versionid 4字节， 删除的个数n1 4字节，(saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节) * n1
    //增加的个数n2 4字节，(number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节) * n2
    public static void analysisVersionBytes(byte[] versionBytes,
                                            Integer[] outVer,
                                            ArrayList<Long> addFileNums,
                                            ArrayList<byte[]> addMinSaxes, ArrayList<byte[]> addMaxSaxes,
                                            ArrayList<Long> addMinTimes, ArrayList<Long> addMaxTimes,
                                            ArrayList<Long> delFileNums,
                                            ArrayList<byte[]> delMinSaxes, ArrayList<byte[]> delMaxSaxes,
                                            ArrayList<Long> delMinTimes, ArrayList<Long> delMaxTimes) {
        assert outVer.length == 1;
        byte[] intBytes = new byte[4], longBytes = new byte[8];
        System.arraycopy(versionBytes, 1, intBytes, 0, 4);
        outVer[0] = bytesToInt(intBytes);
        int numDel, numAdd;
        System.arraycopy(versionBytes, 1 + 4, intBytes, 0, 4);
        numDel = bytesToInt(intBytes);
        System.arraycopy(versionBytes, 1 + 4 + 4 + numDel * 40, intBytes, 0, 4);
        numAdd = bytesToInt(intBytes);
        for (int i = 0; i < numDel; i ++ ) {
            byte[] delMinSax = new byte[Parameters.saxDataSize];
            System.arraycopy(versionBytes, 1 + 4 + 4 + i * 40, longBytes, 0, 8);
            delFileNums.add(bytesToLong(longBytes));

            System.arraycopy(versionBytes, 1 + 4 + 4 + i * 40 + 8, delMinSax, 0, 8);
            delMinSaxes.add(delMinSax);

            byte[] delMaxSax = new byte[Parameters.saxDataSize];
            System.arraycopy(versionBytes, 1 + 4 + 4 + i * 40 + 16, delMaxSax, 0, 8);
            delMaxSaxes.add(delMaxSax);

            System.arraycopy(versionBytes, 1 + 4 + 4 + i * 40 + 24, longBytes, 0, 8);
            delMinTimes.add(bytesToLong(longBytes));

            System.arraycopy(versionBytes, 1 + 4 + 4 + i * 40 + 32, longBytes, 0, 8);
            delMaxTimes.add(bytesToLong(longBytes));
        }

        for (int i = 0; i < numAdd; i ++ ) {
            System.arraycopy(versionBytes, 1 + 4 + 4 + numDel * 40 + 4 + i * 40, longBytes, 0, 8);
            addFileNums.add(bytesToLong(longBytes));

            byte[] addMinSax = new byte[Parameters.saxDataSize];
            System.arraycopy(versionBytes, 1 + 4 + 4 + numDel * 40 + 4 + i * 40 + 8, addMinSax, 0, 8);
            addMinSaxes.add(addMinSax);

            byte[] addMaxSax = new byte[Parameters.saxDataSize];
            System.arraycopy(versionBytes, 1 + 4 + 4 + numDel * 40 + 4 + i * 40 + 16, addMaxSax, 0, 8);
            addMaxSaxes.add(addMaxSax);

            System.arraycopy(versionBytes, 1 + 4 + 4 + numDel * 40 + 4 + i * 40 + 24, longBytes, 0, 8);
            addMinTimes.add(bytesToLong(longBytes));

            System.arraycopy(versionBytes, 1 + 4 + 4 + numDel * 40 + 4 + i * 40 + 32, longBytes, 0, 8);
            addMaxTimes.add(bytesToLong(longBytes));
        }
    }
}
