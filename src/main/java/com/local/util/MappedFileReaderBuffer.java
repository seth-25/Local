package com.local.util;

import com.local.domain.Parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;


public class MappedFileReaderBuffer {
    private MappedByteBuffer[] mappedBufArray;
    private int count = 0;
    private final int number;
    private FileInputStream fileIn;
    private FileChannel fileChannel;
    private final long fileLength;
    private ByteBuffer readTsByteBuf;


    private final int arraySize;
    private byte[] array;
    private byte[][] arrays; // 用于读取时间序列进行插入
    private Queue<byte[]> arraysList = new ConcurrentLinkedQueue<>();   // 用于指定使用哪个array
    private byte[] resArray;
    private byte[] tsArray; // 用于查询原始时间序列，返回单个ts
    byte[][] tsArrays;

    private boolean isRes = false;


    private AsynchronousFileChannel asynchronousFileChannel;
    private List<Long> pList = new ArrayList<>();
    private List<ByteBuffer> byteBufferList = new ArrayList<>();
    private List<Future<Integer>> operationList = new ArrayList<>();

    private int offset = 0;

    private final int fileNum;

    public MappedFileReaderBuffer(String filePath, int arraySize, int fileNum) throws IOException {
        this.fileIn = new FileInputStream(filePath);
        this.fileChannel = fileIn.getChannel();
        this.fileLength = fileChannel.size();

        // 一个内存映射最大的偏移量(ByteBuffer指针用int存，超过2g无法表示).并且确保是readSize的整数倍
//        int maxOffset = Integer.MAX_VALUE / Parameters.FileSetting.readSize * Parameters.FileSetting.readSize;

        this.number = (int) Math.ceil((double) fileLength / (double) Parameters.FileSetting.readSize);    // 向上取整
        this.mappedBufArray = new MappedByteBuffer[number];// 内存文件映射数组
        long preLength = 0;
        long regionSize = Parameters.FileSetting.readSize;// 每块映射区域的大小
        for (int i = 0; i < number; i ++ ) {// 将文件的连续区域映射到内存文件映射数组中
            if (fileLength - preLength < (long) Parameters.FileSetting.readSize) {
                regionSize = fileLength - preLength;// 最后一片区域的大小
            }
            mappedBufArray[i] = fileChannel.map(FileChannel.MapMode.READ_ONLY, preLength, regionSize);
            preLength += regionSize;// 下一片区域的开始
        }
        // 顺序读
        this.arraySize = arraySize;
        this.arrays = new byte[2 * Parameters.insertNumThread][arraySize];
        for (int i = 0; i < 2 * Parameters.insertNumThread; i ++ ) {
            arraysList.offer(arrays[i]);
        }


        // 随机读
        this.tsArray = new byte[Parameters.tsSize];
        this.readTsByteBuf = ByteBuffer.allocate(Parameters.tsSize);

        Path path = Paths.get(filePath);
        asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        for (int i = 0; i < Parameters.FileSetting.queueSize; i ++ ) {
            byteBufferList.add(ByteBuffer.allocateDirect(Parameters.tsSize));
        }
        this.tsArrays = new byte[Parameters.FileSetting.queueSize][Parameters.tsSize];// new byte时间消耗很大，预先开好空间
        this.fileNum = fileNum;
    }

    public ByteBuffer read() {
        array = arraysList.poll();
        if (count >= number) {  // 文件读取完毕
            PrintUtil.print("清空读取文件的byte数组");
            resArray = null;
            arrays = null;
            return null;
        }

        int limit = mappedBufArray[count].limit();
        int position = mappedBufArray[count].position();

        if (limit - position > arraySize) {
            isRes = false;
            offset  += arraySize / Parameters.tsSize;
            MappedByteBuffer mappedByteBuffer = mappedBufArray[count];
            mappedByteBuffer.load();
            while(!mappedByteBuffer.isLoaded());
            return mappedByteBuffer;
        }
        else if (limit - position == arraySize){ // 本内存文件映射最后一次读取数据
//            System.out.println("下一个映射");
            isRes = false;
            offset  += arraySize / Parameters.tsSize;
            MappedByteBuffer mappedByteBuffer = mappedBufArray[count ++];   // 转换到下一个内存文件映射
            mappedByteBuffer.load();
            while(!mappedByteBuffer.isLoaded());
            return mappedByteBuffer;
        }
        else {
            isRes = true;
            if ((limit - position) % Parameters.tsSize != 0) {
                throw new RuntimeException("读取的ts不完整");
            }
            offset  += (limit - position) / Parameters.tsSize;
            MappedByteBuffer mappedByteBuffer = mappedBufArray[count ++ ];
            mappedByteBuffer.load();
            while(!mappedByteBuffer.isLoaded());
            return mappedByteBuffer;
        }
    }
//    public byte[] getArray() {
//        if (!isRes) {
//            return array;
//        }
//        else {
//            return resArray;
//        }
//    }
//    public void arraysListOffer(byte[] array) {
//        arraysList.offer(array);
//    }
//    public byte[] readTs(long offset, int num) {
//        try {
//            fileChannel.read(readTsByteBuf, offset * Parameters.tsSize);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        readTsByteBuf.flip();
//        readTsByteBuf.get(tsArrays[num]);
//        readTsByteBuf.clear();
//        return tsArrays[num];
//    }

    public byte[] readTs(long offset) {
        try {
            fileChannel.read(readTsByteBuf, offset * Parameters.tsSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        readTsByteBuf.flip();
        readTsByteBuf.get(tsArray);
        readTsByteBuf.clear();
        return tsArray;
    }

    public byte[] readTsNewByte(long offset) {
        try {
            fileChannel.read(readTsByteBuf, offset * Parameters.tsSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] oneTs = new byte[Parameters.tsSize];
        readTsByteBuf.flip();
        readTsByteBuf.get(oneTs);
        readTsByteBuf.clear();
        return oneTs;
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

    public long getFileLength() {
        return fileLength;
    }

    public int getOffset() {
        return offset;
    }

    public int getFileNum() {
        return fileNum;
    }

    public void close() throws IOException {
        fileIn.close();
        arrays = null;
        resArray = null;
    }
}
