package com.local.domain;

import java.nio.charset.StandardCharsets;

//public class LeafTimeKey implements Comparable<LeafTimeKey>{
public class LeafTimeKey{
    private byte[] saxT;
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

    public LeafTimeKey(byte[] saxT, byte p_hash, byte[] p_offset, byte[] timeStamp) {
        this.saxT = saxT;
        this.p_offset = p_offset;
        this.p_hash = p_hash;
        this.timeStamp = timeStamp;
    }

    public byte[] getSaxT(){
        return saxT;
    }

    public byte[] getTimeStamp() {
        return timeStamp;
    }

    public byte[] getLeafTimeKeys() {
        byte[] res = new byte[saxT.length + 1 + p_offset.length + timeStamp.length];
        System.arraycopy(p_offset, 0, res, 0, p_offset.length);
        res[7] = p_hash;
        System.arraycopy(saxT, 0, res, 1 + p_offset.length, saxT.length);
        System.arraycopy(timeStamp, 0, res, saxT.length + 1 + p_offset.length, timeStamp.length);
        return res;
    }

    public int getSaxLength() {
        return saxT.length;
    }


    @Override
    public String toString() {
        return new String(saxT, StandardCharsets.UTF_8);
    }
}
