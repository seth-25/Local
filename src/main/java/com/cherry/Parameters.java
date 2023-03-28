package com.cherry;

public class Parameters {

    public static String hostName = "Ubuntu001";

    // Whether the data contains a timestamp, 0:No timestamp, 1:has timestamp but not store, 2: has timestamp and store
    public static final int hasTimeStamp = 0;
    // Size of time series (bytes)
    public static final int tsDataSize = 256 * 4;
    // Size of timeStamp (bytes)
    public static final int timeStampSize = 8;
    public static final int tsSize = (hasTimeStamp > 0) ? tsDataSize + timeStampSize : tsDataSize;


    // The number of segments a time series is divided into, support 8 and 16. Corresponds to `is8_segment` in `leveldb/sax/include/globals.h`
    public static final int segmentSize = 16;
    // The number of paa. Each segment is represented by a paa
    public static final int paaNum = segmentSize;
    // The number of bits in the binary representation of the PAA discretization.
    public static final int bitCardinality = 8;
    // Size of saxT (bytes)
    public static final int saxTSize = segmentSize * bitCardinality / 8;
    // The size of the pointers in leaf key, 7 bytes for p_offset (to record the position of the TS in the file) + 1 byte for p_hash (to record the file name).
    public static final int pointerSize = 8;
    // Size of leaf key
    public static final int leafTimeKeysSize = saxTSize + pointerSize + ((hasTimeStamp > 0) ? timeStampSize : 0);


    public static class FileSetting {
        // The folder where time series is stored, the program will automatically read all the files in that folder
        public static final String inputPath = "../ts/";
        // File path of the query file.
        public static final String queryFilePath = "../query/query.bin";
        // The number of time series read at once from the file.
        public static final int readTsNum = 100000;
        // The maximum number of times to read a file.
        public static final int readLimit = 100;
        // The size (in bytes) to read at once when reading a file.
        public static final int readSize = tsSize * readTsNum;
        // The length of the queue for random reads. (GAL step3)
        public static final int queueSize = 1024;
    }


    /**
     * Init and Insert
     */

    // The number of times to read a file for init, ensuring that `initNum` * `readTsNum` = `init_num` in `leveldb/sax/include/globals.h`
    public static final int initNum = 10;
    // The number of threads used for insertion. Corresponds to `pool_size` in `leveldb/sax/include/globals.h`
    public static final int insertNumThread = 1;




    /**
     * Search
     */
    // The maximum number of Pos stored in the queue for accessing time series. Corresponds to `info_p_max_size` in `leveldb/sax/include/globals.h`
    // When the number of time series accessed during each exact query is high, it is necessary to increase the value.
    public static final int infoMaxPSize = 10240;
//    public static final int infoMaxPSize = 250000000;

    // Sort the Pos before accessing time series. When `sort_p` in `leveldb/sax/include/globals.h` is 1, it should be false.
    // Because sort_p=1, the incoming request has already been sorted by Pos, so there is no need to sort it again.
    // GAL(step 2)
    public static final boolean SortPos = true;




    // size of approximate query result (has p)
    // appro_res(has timestamp): ts 256*4, long time 8, float dist 4, empty 4 (time is long, alignment), long p 8
    // appro_res(no timestamp): ts 256*4, float dist 4, empty 4 (p is long, alignment), long p 8
    public static final int approximateResSize = tsSize + 16;
    // size of exact query result (no p)
    // exact_res(has timestamp): ts 256*4, long time 8, float dist 4, empty 4 (time is long, alignment)
    // exact_res(no timestamp): ts 256*4, float dist 4
    public static final int exactResSize = tsSize + ((hasTimeStamp > 0) ? 8 : 4);

    // Whether to print debug information
    public static final boolean debug = false;

    // Whether to store saxT in little-endian
    public static final boolean isSuffix = true;
}
