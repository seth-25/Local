package com.cherry.util;

import com.cherry.domain.Parameters;

import java.util.Arrays;

public class SaxTUtil {
    public static byte[] createPointerOffset(long offset) {
        byte[] p_offset = new byte[Parameters.pointerSize - 1];
        for (int i = 0; i < Parameters.pointerSize - 1; i ++ ) {
            p_offset[i] = (byte) (offset >> i * 8);  // 小端 从long的低位开始截断，放在地址低的地方
        }
        return p_offset;
    }

    public static long pointerOffsetToLong(byte[] p_offset) {
        long l = 0;
        for (int i = 0; i < p_offset.length; i ++ ) {
            l <<= 8;
            l |= (p_offset[p_offset.length - 1 - i] & 0xff);  // 小端
        }
        return l;
    }


    public static int compareSaxT(byte[] a, byte[] b) {
//        System.out.println(a.length + " " + b.length);
        assert a.length == b.length;
        if (Parameters.isSuffix) {
            for (int i = a.length - 1; i >= 0; i -- ) { // 小端
                if ((a[i] & 0xff) < (b[i] & 0xff)) return -1;
                else if ((a[i] &0xff) > (b[i] & 0xff)) return 1;
            }
        }
        else {
            for (int i = 0; i < a.length; i ++ ) { // 小端
                if ((a[i] & 0xff) < (b[i] & 0xff)) return -1;
                else if ((a[i] &0xff) > (b[i] & 0xff)) return 1;
            }
        }

        return 0;
    }

    public static byte[] makeMinSaxT(byte[] saxT, int d) {    // 根据相距度截取saxT,不足的填充0
        byte[] minSaxT = new byte[saxT.length];
        byte[] zeroBytes = new byte[saxT.length - d * Parameters.segmentSize / 8];
        Arrays.fill(zeroBytes, (byte) 0x00);
        if (Parameters.isSuffix) {
            System.arraycopy(saxT, saxT.length - d * Parameters.segmentSize / 8, minSaxT, saxT.length - d * Parameters.segmentSize / 8, d * Parameters.segmentSize / 8);
            System.arraycopy(zeroBytes, 0, minSaxT, 0, zeroBytes.length);
        }
        else {
            System.arraycopy(saxT, 0, minSaxT, 0, d * Parameters.segmentSize / 8);
            System.arraycopy(zeroBytes, 0, minSaxT, d * Parameters.segmentSize / 8, zeroBytes.length);
        }
        return minSaxT;
    }
    public static byte[] makeMaxSaxT(byte[] saxT, int d) {    // 根据相距度截取saxT,不足的填充0xff
        byte[] maxSaxT = new byte[saxT.length];
        byte[] maxBytes = new byte[saxT.length - d * Parameters.segmentSize / 8];
        Arrays.fill(maxBytes, (byte) 0xff);
        if (Parameters.isSuffix) {
            System.arraycopy(saxT, saxT.length - d * Parameters.segmentSize / 8, maxSaxT, saxT.length - d * Parameters.segmentSize / 8, d * Parameters.segmentSize / 8);
            System.arraycopy(maxBytes, 0, maxSaxT, 0, maxBytes.length);
        }
        else {
            System.arraycopy(saxT, 0, maxSaxT, 0, d * Parameters.segmentSize / 8);
            System.arraycopy(maxBytes, 0, maxSaxT, d * Parameters.segmentSize / 8, maxBytes.length);
        }
        return maxSaxT;
    }
}
