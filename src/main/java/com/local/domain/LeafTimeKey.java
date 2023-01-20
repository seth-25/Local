package com.local.domain;

import java.nio.charset.StandardCharsets;

public class LeafTimeKey implements Comparable<LeafTimeKey>{
    private byte[] sax;
    private byte p_hash;
    private byte[] p_offset;
    private byte[] timeStamp;

//    public Sax(byte[] leafTimeKeys) {
//        this.data = new byte[Parameters.saxDataSize];
//        this.p_offset = new byte[Parameters.saxPointerSize - 1];
//        this.timeStamp = new byte[Parameters.timeStampSize];
//        p_hash = leafTimeKeys[0];
//        System.arraycopy(leafTimeKeys, 1, p_offset, 0, p_offset.length);
//        System.arraycopy(leafTimeKeys, 1 + p_offset.length, data, 0, data.length);
//        System.arraycopy(leafTimeKeys, data.length + 1 + p_offset.length, timeStamp, 0, timeStamp.length);
//    }

    public LeafTimeKey(byte[] saxData, byte p_hash, byte[] p_offset, byte[] timeStamp) {
        this.sax = saxData;
        this.p_offset = p_offset;
        this.p_hash = p_hash;
        this.timeStamp = timeStamp;
    }

    public byte[] getSax(){
        return sax;
    }

    public byte[] getTimeStamp() {
        return timeStamp;
    }

    public byte[] getLeafTimeKeys() {
        byte[] res = new byte[sax.length + 1 + p_offset.length + timeStamp.length];
        System.arraycopy(p_offset, 0, res, 0, p_offset.length);
        res[7] = p_hash;
        System.arraycopy(sax, 0, res, 1 + p_offset.length, sax.length);
        System.arraycopy(timeStamp, 0, res, sax.length + 1 + p_offset.length, timeStamp.length);
        return res;
    }

    public int getSaxLength() {
        return sax.length;
    }

    @Override
    public int compareTo(LeafTimeKey o) {
        assert this.getSaxLength() == o.getSaxLength();
        byte[] a = this.getSax();
        byte[] b = o.getSax();
        for (int i = a.length - 1; i >= 0; i -- ) { // 小端
            if ((a[i] & 0xff) < (b[i] & 0xff)) return -1;
            else if ((a[i] &0xff) > (b[i] & 0xff)) return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return new String(sax, StandardCharsets.UTF_8);
    }
}
