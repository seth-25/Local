package com.local.util;

import com.local.domain.Parameters;

public class SaxUtil {
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
}
