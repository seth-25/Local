package com.local.domain;

public class Parameters {

    public static final int hasTimeStamp = 0;   // 0没时间戳,1有且不存时间戳,2有且存时间戳

    public static String hostName = "Ubuntu002"; // 本机的hostname
    public static int numThread = 20;

    public static final int timeSeriesDataSize = 256 * 4;   // 时间序列多少字节
    public static final int timeStampSize = 8; // 时间戳多少字节
    public static final int tsSize = (hasTimeStamp > 0) ? timeSeriesDataSize + timeStampSize : timeSeriesDataSize;
    public static final int tsHash = 256;   // 时间戳哈希取余大小


    public static final int segmentSize = 8;   // 分成几段
    public static final int paaNum = segmentSize; // paa个数,一段一个paa
    public static final int bitCardinality = 8; // paa离散化几位
    public static final int saxTSize = segmentSize * bitCardinality / 8; // saxT多少字节
    public static final int pointerSize = 8; // LeafTimeKeys中指针的大小,7字节p_offset+1字节p_hash
    public static final int LeafTimeKeysSize = saxTSize + pointerSize + timeStampSize; // 一个LeafTimeKeys结构大小多少字节

    // 查询原始时间序列的结果 ares(含p)
    // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8, 总共1048
    // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8, 总共1040
    public static final int aresSize = tsSize + 16;
    //查询原始时间序列的结果 ares_exact(不含p)
    // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐) 总共1040
    // ares_exact(没时间戳): ts 256*4, float dist 4, 总共1028
    public static final int aresExactSize = tsSize + ((hasTimeStamp > 0) ? 8 : 4);
    public static final int findOriTsNum = 2000; // 一次访问原始时间序列个数

    public static class FileSetting {
        public static final int readTsNum = 1000000; // 读取文件时一次读的ts数量
        public static final int readSize = tsSize * readTsNum; // 读取文件时一次读取字节数
        public static final String inputPath = "./ts/"; // 存储ts的文件夹
        public static final String queryFilePath = "./query/query.bin"; // 存储查询的ts的文件
    }


    public static final boolean isSearchMultithread = true; // 查询是否多线程
    public static final boolean isSuffix = true;
    public static final boolean debug = false;
}
