package com.local.insert;

import com.local.util.MappedFileReader;

class TsReadBatch {
    private final byte[] tsBytes;
    private final int fileNum;
    private final long offset;
    private MappedFileReader reader;
    public TsReadBatch(byte[] tsBytes, int fileNum, long offset, MappedFileReader reader) {
        this.tsBytes = tsBytes;
        this.fileNum = fileNum;
        this.offset = offset;
        this.reader = reader;
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

    public MappedFileReader getReader() {
        return reader;
    }
}
