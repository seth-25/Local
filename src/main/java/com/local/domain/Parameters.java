package com.local.domain;

public class Parameters {

    public static final int hasTimeStamp = 2;   // 0没时间戳,1有且不存时间戳,2有且存时间戳

    public static String hostName = "Ubuntu002"; // 本机的hostname
    public static final int tsDataSize = 256 * 4;   // 时间序列多少字节
    public static final int timeStampSize = 8; // 时间戳多少字节
    public static final int tsSize = (hasTimeStamp > 0) ? tsDataSize + timeStampSize : tsDataSize;


    public static final int segmentSize  = 16;   // 分成几段
    public static final int paaNum = segmentSize; // paa个数,一段一个paa
    public static final int bitCardinality = 8; // paa离散化几位
    public static final int saxTSize = segmentSize * bitCardinality / 8; // saxT多少字节
    public static final int pointerSize = 8; // LeafTimeKeys中指针的大小,7字节p_offset+1字节p_hash
    public static final int leafTimeKeysSize = saxTSize + pointerSize + ((hasTimeStamp > 0) ? timeStampSize : 0); // 一个LeafTimeKeys结构大小多少字节

    ////////////////////////Search
    // 查询原始时间序列的结果 ares(含p)
    // appro_res(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
    // appro_res(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
    public static final int approximateResSize = tsSize + 16;
    //查询原始时间序列的结果 ares_exact(不含p)
    // exact_res(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
    // exact_res(有时间戳): ts 256*4, float dist 4
    public static final int exactResSize = tsSize + ((hasTimeStamp > 0) ? 8 : 4);
    public static final int infoMaxPSize = 1024;   // 最多一次性查询多少个原始时间序列
    public static final boolean findOriTsSort = true; // 批量查询原始时间序列，是否排序
    ///////////////////////Init
    public static final int initNum = 10;    // 初始化读取几次,保证initNum * readTsNum = leveldb/sax/include/globals.h的 init_num
    public static final int insertNumThread = 1;    // 插入的线程，和leveldb/sax/include/globals.h的pool_size相同


    public static class FileSetting {
        public static final int readTsNum = 100000; // 读取文件时一次读的ts数量
        public static final int readSize = tsSize * readTsNum; // 读取文件时一次读取字 节数
        public static final String inputPath = "../ts/"; // 存储ts的文件夹
        public static final String queryFilePath = "../query/query.bin"; // 存储查询的ts的文件
        public static final int queueSize = 32; // 随机读写队列长度

    }

    public static final boolean isSuffix = true;
    public static final boolean debug = false;
}
