package com.local.domain;

public class Parameters {

    public static String hostName = "Ubuntu002"; // 本机的hostname
    public static int numThread = 20;

    public static final int timeSeriesDataSize = 256 * 4;   // 时间序列的大小
    public static final int timeStampSize = 8; // 时间戳大小
    public static final int tsSize = timeSeriesDataSize + timeStampSize;
    public static final int tsHash = 256;   // 时间戳哈希取余大小



    public static final int segmentSize = 16;   // 分成几段
    public static final int paaSize = segmentSize; // paa个数,一段一个paa
    public static final int bitCardinality = 8; // paa离散化几位
    public static final int saxSize = segmentSize * bitCardinality / 8; // sax多少字节
    public static final int pointerSize = 8; // LeafTimeKeys中指针的大小,7字节p_offset+1字节p_hash
    public static final int LeafTimeKeysSize = saxSize + pointerSize + timeStampSize; // 一个LeafTimeKeys结构大小多少字节



    public static class FileSetting {
        public static final int readTsNum = 1000000; // 读取文件时一次读的ts数量
        public static final int readSize = tsSize * readTsNum; // 读取文件时一次读取字节数
        public static final String inputPath = "./ts/"; // 存储ts的文件夹

    }
}
