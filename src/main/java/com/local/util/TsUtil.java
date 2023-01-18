package com.local.util;

import com.local.domain.Parameters;
import com.local.domain.TimeSeries;

public class TsUtil {
    public static int computeHash(TimeSeries timeSeries) {
        byte[] timeStampByte = timeSeries.getTimeStamp();
        return (int) (bytesToLong(timeStampByte) % Parameters.tsHash);
    }

    public static long bytesToLong(byte[] bytes) {
        long l = 0;
        for (int i = 0; i < Parameters.timeStampSize; i ++ ) {
            l <<= 8;
            l |= (bytes[Parameters.timeStampSize - 1 - i] & 0xff);  // 小端
        }
        return l;
    }

    public static long bytesToLong(byte[] bytes, int offset) {
        long l = 0;
        for (int i = 0; i < Parameters.timeStampSize; i ++ ) {
            l <<= 8;
            l |= (bytes[offset + Parameters.timeStampSize - 1 - i] & 0xff);  // 小端
        }
        return l;
    }

    public static byte[] longToBytes(long l) {
        byte[] bytes = new byte[Parameters.timeStampSize];
        for (int i = 0; i < Parameters.timeStampSize; i ++ ) {
            bytes[i] = (byte) (l >> (i * 8));   // 小端，截断低位给bytes低地址
        }
        return bytes;
    }
}
