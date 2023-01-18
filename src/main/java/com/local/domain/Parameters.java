package com.local.domain;

public class Parameters {

    public static String hostName = "Ubuntu002"; // 本机的hostname
    public static int numThread = 20;

    public static final int timeSeriesDataSize = 256 * 4;   // 时间序列的大小
    public static final int timeStampSize = 8; // 时间戳大小
    public static final int tsSize = timeSeriesDataSize + timeStampSize;
    public static final int tsHash = 256;   // 时间戳哈希取余大小



    public static final int saxDataSize = 8; // sax中数据大小
    public static final int saxPointerSize = 8; // sax中指针的大小,7字节p_offset+1字节p_hash
    public static final int saxSize = saxDataSize + saxPointerSize + timeStampSize; // 一条sax大小多少字节


    public static class Init {
        public static final int numTs = 1000000; // 初始化ts的个数
    }
    public static class Insert {
        public static final int numTempTs = 3; // 暂存几个Ts，达到个数后开始发送
    }

    public static class FileSetting {
        //        public static final int readSize = tsSize * 10000; // 读取文件时一次读取字节数
        public static final int readSize = tsSize * 100000; // 读取文件时一次读取字节数
        public static final String inputPath = "./ts/"; // 存储ts的文件夹

    }
}
