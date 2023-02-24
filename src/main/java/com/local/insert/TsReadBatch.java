package com.local.insert;

import com.local.util.MappedFileReader;

import java.nio.ByteBuffer;

class TsReadBatch {
    private final ByteBuffer tsBuffer;
    private final int fileNum;
    private final long offset;
    public TsReadBatch(ByteBuffer tsBuffer, int fileNum, long offset) {
        this.tsBuffer = tsBuffer;
        this.fileNum = fileNum;
        this.offset = offset;
    }

    public int getFileNum() {
        return fileNum;
    }

    public ByteBuffer getTsBuffer() {
        return tsBuffer;
    }

    public long getOffset() {
        return offset;
    }

}
