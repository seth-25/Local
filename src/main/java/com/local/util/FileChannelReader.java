package com.local.util;

import com.local.domain.Parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelReader {
    private FileInputStream fileIn;
    private ByteBuffer byteBuf;
    private ByteBuffer readTsByteBuf;
    private long fileLength;
    private int offset;
    private int fileNum;
    private byte[] array;
    private FileChannel fileChannel;
    public FileChannelReader(String fileName, int arraySize, int fileNum) throws IOException {
        this.fileIn = new FileInputStream(fileName);
        this.fileLength = fileIn.getChannel().size();
        this.byteBuf = ByteBuffer.allocate(arraySize);  // Buffer大小（字节）
        this.readTsByteBuf = ByteBuffer.allocate(Parameters.tsSize);
        this.fileChannel = fileIn.getChannel();
        this.fileNum = fileNum;
    }


    public int read() {
        int bytes = 0;// 读取到ByteBuffer中
        try {
            bytes = fileChannel.read(byteBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (bytes != -1) {

            int oldOffset = offset;
            offset += bytes / Parameters.tsSize;

            array = new byte[bytes];// 字节数组长度为已读取长度
            byteBuf.flip();
            byteBuf.get(array);// 从ByteBuffer中得到字节数组
            byteBuf.clear();
            return oldOffset;
        }
        return -1;
    }

    public byte[] readTs(long offset) {
        try {
            fileChannel.read(readTsByteBuf, offset * Parameters.tsSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] ts = new byte[Parameters.tsSize];
        readTsByteBuf.flip();
        readTsByteBuf.get(ts);
        readTsByteBuf.clear();
        return ts;
    }

    public void close() throws IOException {
        fileIn.close();
        array = null;
    }

    public byte[] getArray() {
        return array;
    }

    public long getFileLength() {
        return fileLength;
    }


    public int getFileNum() {
        return fileNum;
    }
}
