package com.local.util;

import com.local.domain.Parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class FileChannelReader {
    private FileInputStream fileIn;
    private ByteBuffer byteBuf;
//    private ByteBuffer readTsByteBuf;
    private long fileLength;
    private long offset = 0;
    private int fileNum;
//    private byte[] array;
//    private byte[] oneTsArray;
    private FileChannel fileChannel;

    private AsynchronousFileChannel asynchronousFileChannel;
    private List<Long> pList = new ArrayList<>();
    private List<ByteBuffer> byteBufferList = new ArrayList<>();
    private List<Future<Integer>> operationList = new ArrayList<>();

    private int arraySize;
    public FileChannelReader(String filePath, int arraySize, int fileNum) throws IOException {
        this.fileIn = new FileInputStream(filePath);
        this.fileChannel = fileIn.getChannel();
        this.fileLength = fileChannel.size();
        this.byteBuf = ByteBuffer.allocateDirect(arraySize);  // Buffer大小（字节）
//        this.readTsByteBuf = ByteBuffer.allocate(Parameters.tsSize);
//        this.array = new byte[arraySize];
//        this.oneTsArray = new byte[Parameters.tsSize];
        Path path = Paths.get(filePath);
        asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        for (int i = 0; i < Parameters.FileSetting.queueSize; i ++ ) {
            byteBufferList.add(ByteBuffer.allocateDirect(Parameters.tsSize));
        }
        this.fileNum = fileNum;
        this.arraySize = arraySize;
    }


//    public int read() {
//        int bytes = 0;// 读取到ByteBuffer中
//        try {
//            bytes = fileChannel.read(byteBuf);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        if (bytes != -1) {
//            int oldOffset = offset;
//            offset += bytes / Parameters.tsSize;
//            byteBuf.flip();
//            byteBuf.get(array);// 从ByteBuffer中得到字节数组
//            byteBuf.clear();
//            return oldOffset;
//        }
//        return -1;
//    }
    public ByteBuffer read() {
        int bytes = 0;
//        ByteBuffer byteBuf;
        try {
//            byteBuf.clear();
//            byteBuf = ByteBuffer.allocateDirect(this.arraySize);
            System.out.println(byteBuf);
            bytes = fileChannel.read(byteBuf);
        }   catch (IOException e) {
            throw  new RuntimeException(e);
        }
        if (bytes != -1) {
            offset += bytes / Parameters.tsSize;
            byteBuf.flip();
            return byteBuf;
        }
        return null;
    }
    public List<Long> getPList() {
        return pList;
    }
    public void clearPList() {
        pList.clear();
    }
    public List<ByteBuffer> readTsQueue() {
//        byte[][] tsArrays = new byte[Parameters.FileSetting.queueSize][Parameters.tsSize];
        int num = pList.size();
//        System.out.println("pList size " + num);
        for (int i = 0; i < num; i ++ ) {
            long offset = pList.get(i) & 0x00ffffffffffffffL;  // ts在文件中的位置
            ByteBuffer byteBuf = byteBufferList.get(i);
            byteBuf.clear();
            Future<Integer> operation = asynchronousFileChannel.read(byteBuf, offset* Parameters.tsSize);   // 异步读取构成队列
            operationList.add(operation);
        }
        for (int i = 0; i < num; i ++ ) {
            Future<Integer> operation = operationList.get(i);
            while (!operation.isDone()) ;   // 等待读完
            byteBufferList.get(i).rewind();
        }
        operationList.clear();

        return byteBufferList;
    }

//
//    public byte[] readTs(long offset) {
//        try {
//            fileChannel.read(readTsByteBuf, offset * Parameters.tsSize);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        readTsByteBuf.flip();
//        readTsByteBuf.get(oneTsArray);
//        readTsByteBuf.clear();
//        return oneTsArray;
//    }

    public void close() throws IOException {
        fileIn.close();
//        array = null;
//        oneTsArray = null;
    }

//    public byte[] getArray() {
//        return array;
//    }

    public long getFileLength() {
        return fileLength;
    }

    public long getOffset() {
        return offset;
    }

    public int getFileNum() {
        return fileNum;
    }
}
