package com.local.util;

import com.local.domain.Parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MappedFileReader {
    private MappedByteBuffer[] mappedBufArray;
    private int count = 0;
    private int number;
    private FileInputStream fileIn;
    private FileChannel fileChannel;
    private long fileLength;
    private ByteBuffer readTsByteBuf;


    private int arraySize;
    private byte[] array;
    private byte[] resArray;
    boolean isRes = false;

    private byte[] oneTsArray;
    private int offset = 0;

    private int fileNum;

    public MappedFileReader(String fileName, int arraySize, int fileNum) throws IOException {
        this.fileIn = new FileInputStream(fileName);
        this.fileChannel = fileIn.getChannel();
        this.fileLength = fileChannel.size();

        // 一个内存映射最大的偏移量(ByteBuffer指针用int存，超过2g无法表示).并且确保是ts的整数倍
        int maxOffset = Integer.MAX_VALUE / Parameters.tsSize * Parameters.tsSize;

        this.number = (int) Math.ceil((double) fileLength / (double) maxOffset);    // 向上取整
        this.mappedBufArray = new MappedByteBuffer[number];// 内存文件映射数组
        long preLength = 0;
        long regionSize = maxOffset;// 映射区域的大小
        for (int i = 0; i < number; i++) {// 将文件的连续区域映射到内存文件映射数组中
            if (fileLength - preLength < (long) maxOffset) {
                regionSize = fileLength - preLength;// 最后一片区域的大小
            }
            mappedBufArray[i] = fileChannel.map(FileChannel.MapMode.READ_ONLY, preLength, regionSize);
//            System.out.println("lim " + mappedBufArray[i].limit());
            preLength += regionSize;// 下一片区域的开始
        }
        this.arraySize = arraySize;
        this.array = new byte[arraySize];
        this.oneTsArray = new byte[Parameters.tsSize];

        this.readTsByteBuf = ByteBuffer.allocate(Parameters.tsSize);

        this.fileNum = fileNum;
    }

    public int read() {
        if (count >= number) {  // 文件读取完毕
            resArray = null;
            array = null;
            return -1;
        }

        int limit = mappedBufArray[count].limit();
        int position = mappedBufArray[count].position();

        int oldOffset = offset;

        if (limit - position > arraySize) {
            isRes = false;
            offset  += arraySize / Parameters.tsSize;
            mappedBufArray[count].get(array);
        }
        else if (limit - position == arraySize){ // 本内存文件映射最后一次读取数据
//            System.out.println("下一个映射");
            isRes = false;
            offset  += arraySize / Parameters.tsSize;
            mappedBufArray[count].get(array);
            if (count < number) {
                count++; // 转换到下一个内存文件映射
            }
        }
        else {
            isRes = true;
            if ((limit - position) % Parameters.tsSize != 0) {
                throw new RuntimeException("读取的ts不完整");
            }
            offset  += (limit - position) / Parameters.tsSize;
            resArray = new byte[limit - position];
            mappedBufArray[count].get(resArray);
            if (count < number) {
                count++; // 转换到下一个内存文件映射
            }
        }
        return oldOffset;
    }

    public byte[] readTs(long offset) {
        System.out.println("读取offset: " + offset);
        try {
            fileChannel.read(readTsByteBuf, offset * Parameters.tsSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        System.out.println("实际位置" + offset * Parameters.tsSize + " " + fileLength + " read成功 " + oneTsArray.length + " " + Thread.currentThread().getName());
        readTsByteBuf.flip();
        readTsByteBuf.get(oneTsArray);
//        System.out.println("写入成功");
        readTsByteBuf.clear();
        return oneTsArray;
    }

    public void close() throws IOException {
        fileIn.close();
        array = null;
        resArray = null;
    }

    public byte[] getArray() {
        if (!isRes) return array;
        else return resArray;
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
}
