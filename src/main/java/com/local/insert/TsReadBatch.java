package com.local.insert;

class TsReadBatch {
    private final byte[] tsBytes;
    private final int fileNum;
    private final long offset;
    public TsReadBatch(byte[] tsBytes, int fileNum, long offset) {
        this.tsBytes = tsBytes;
        this.fileNum = fileNum;
        this.offset = offset;
    }

    public int getFileNum() {
        return fileNum;
    }

    public byte[] getTsBytes() {
        return tsBytes;
    }

    public long getOffset() {
        return offset;
    }
}
