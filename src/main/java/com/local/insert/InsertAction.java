package com.local.insert;

import com.local.domain.Parameters;
import com.local.domain.LeafTimeKey;
import com.local.util.*;

import java.util.ArrayList;

public class InsertAction {

//    public static boolean readTs(int num) throws IOException {  // 创建putSaxThread个list，每个list存一堆ts
//        long t1 = System.currentTimeMillis(); // todo
//        CacheUtil.readTimeSeries = new ArrayList<>();
//        int cnt = 0;
//        while (cnt < num) {
//            int read_state = reader.read();
//
//            if (read_state == -1) { // 文件读完了
//                reader.close();
//                return true;
//            }
//
//            System.out.println("读:" + cnt);
//
//            byte[] arrays = reader.getArray();
//            ArrayList<TimeSeries> timeSeriesArrayList = InsertAction.createTimeSeries(arrays);
//            CacheUtil.readTimeSeries.add(timeSeriesArrayList);
//            cnt ++ ;
//        }
//        Main1.readTime += System.currentTimeMillis() - t1; // todo
//        return false;
//    }


//    public static ArrayList<TimeSeries> createTimeSeries(byte[] arrays) {
//        ArrayList<TimeSeries> timeSeriesArrayList = new ArrayList<>();
//        int index = 0;
//        while (index < arrays.length) {
//            byte[] tsDataByte = new byte[Parameters.timeSeriesDataSize]; // 一个ts的byte形式
//            System.arraycopy(arrays, index, tsDataByte, 0, Parameters.timeSeriesDataSize);
////            long timeStamp = new Date().getTime() / 1000;
//            long timeStamp = 1;
//            TimeSeries  timeSeries = new TimeSeries(tsDataByte, TsUtil.longToBytes(timeStamp));
//            timeSeriesArrayList.add(timeSeries);
//            index += Parameters.timeSeriesDataSize;
//        }
//
//        return timeSeriesArrayList;
//    }

//    public static Sax tsToSax(TimeSeries timeSeries) throws IOException {
////        System.out.println("worker收到ts,将ts写入文件,并转化成sax");
//        long offset = 0;
//        synchronized (InsertAction.class) {
//            offset = writer.writeTs(timeSeries);
//        }
//        byte[] saxData = new byte[Parameters.saxDataSize];
//        DBUtil.dataBase.saxt_from_ts(timeSeries.getTimeSeriesData(), saxData);
//        return new Sax(saxData, (byte) TsUtil.computeHash(timeSeries), SaxUtil.createPointerOffset(offset), timeSeries.getTimeStamp());
//    }


//
//    public static Sax tsToSax(TimeSeries timeSeries, RandomAccessFile randomAccessFile) throws IOException {
////        System.out.println("worker收到ts,将ts写入文件,并转化成sax");
//        long offset = 0;
//        synchronized (InsertAction.class) {
//            offset = FileUtil.writeTs(timeSeries, randomAccessFile);
//
//        }
//        byte[] saxData = new byte[Parameters.saxDataSize];
//        DBUtil.dataBase.saxt_from_ts(timeSeries.getTimeSeriesData(), saxData);
//        return new Sax(saxData, (byte) TsUtil.computeHash(timeSeries), SaxUtil.createPointerOffset(offset), timeSeries.getTimeStamp());
//    }


//    public static void putSax(Sax sax) {
//        System.out.println(sax.getLeafTimeKeys().length);
//        System.out.println(Arrays.toString(sax.getLeafTimeKeys()));

//        DBUtil.dataBase.put(sax.getLeafTimeKeys());
//        System.out.println("sax存储完成");
//    }




//////////////////////////////////////////////////////////////////////
//    static int cnt = 0;
//    public static byte[] readTs(FileChannelReader reader) {  // 创建putSaxThread个list，每个list存一堆ts
//        long t1 = System.currentTimeMillis(); // todo
//
//        int read_state = 0;
//        try {
//            read_state = reader.read();
//            if (read_state == -1) { // 文件读完了
//                reader.close();
//                return new byte[0];
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        System.out.println("读:" + cnt);
//        Main1.readTime += System.currentTimeMillis() - t1; // todo
//
//        byte[] arrays = reader.getArray();
////        ArrayList<TimeSeries> timeSeriesArrayList = InsertAction.createTimeSeries(arrays);
//        cnt ++ ;
//
//        return arrays;
//    }



//    public static long putTss(ArrayList<TimeSeries> tsList){
////        System.out.println("worker收到ts,将ts写入文件,并转化成sax");
//        long offset = 0;
//        byte[] tssBytes = new byte[Parameters.tsSize * tsList.size()];
//        int cnt = 0;
//        for (TimeSeries ts: tsList) {
//            System.arraycopy(ts.getTimeSeriesData(), 0, tssBytes, cnt * Parameters.tsSize, Parameters.timeSeriesDataSize);
//            System.arraycopy(ts.getTimeStamp(), 0, tssBytes, cnt * Parameters.tsSize + Parameters.timeSeriesDataSize, Parameters.timeStampSize);
//            cnt ++ ;
//        }
//        synchronized (InsertAction.class) {
//            try {
//                writer.write(tssBytes);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        return 0;
//    }
//
//    public static long putTss(ArrayList<TimeSeries> tsList, FileEnv fileEnv) {
////        System.out.println("worker收到ts,将ts写入文件,并转化成sax");
//        long offset = 0;
//        byte[] tssBytes = new byte[Parameters.tsSize * tsList.size()];
//        int cnt = 0;
//        for (TimeSeries ts: tsList) {
//            System.arraycopy(ts.getTimeSeriesData(), 0, tssBytes, cnt * Parameters.tsSize, Parameters.timeSeriesDataSize);
//            System.arraycopy(ts.getTimeStamp(), 0, tssBytes, cnt * Parameters.tsSize + Parameters.timeSeriesDataSize, Parameters.timeStampSize);
//            cnt ++ ;
//        }
//        synchronized (InsertAction.class) {
//            offset = fileEnv.add(tssBytes, 0);
//        }
//
//        return offset;
//    }

    public static ArrayList<LeafTimeKey> getSaxes(byte[] tsBytes, int fileNum, long offset) {
        byte[] leafTimeKeysBytes = DBUtil.dataBase.leaftimekey_from_tskey(tsBytes, fileNum, offset, false);
        ArrayList<LeafTimeKey> leafTimeKeys = new ArrayList<>();
        for (int i = 0; i < leafTimeKeysBytes.length / Parameters.LeafTimeKeysSize; i ++ ) {
            byte[] data = new byte[Parameters.saxSize];
            byte[] p_offset = new byte[Parameters.pointerSize - 1];
            byte[] timeStamp = new byte[Parameters.timeStampSize];
            System.arraycopy(leafTimeKeysBytes, Parameters.LeafTimeKeysSize * i, p_offset, 0, p_offset.length);
            byte p_hash = leafTimeKeysBytes[Parameters.LeafTimeKeysSize * i + p_offset.length];
            System.arraycopy(leafTimeKeysBytes, Parameters.LeafTimeKeysSize * i + 1 + p_offset.length, data, 0, data.length);
            System.arraycopy(leafTimeKeysBytes, Parameters.LeafTimeKeysSize * i + data.length + 1 + p_offset.length, timeStamp, 0, timeStamp.length);
            LeafTimeKey leafTimeKey = new LeafTimeKey(data, p_hash, p_offset, timeStamp);
            leafTimeKeys.add(leafTimeKey);
        }
        return leafTimeKeys;
    }

    public static byte[] getLeafTimeKeysBytes(byte[] tsBytes, int fileNum, long offset) {
        return DBUtil.dataBase.leaftimekey_from_tskey(tsBytes, fileNum, offset, false);
    }
    public static byte[] getLeafTimeKeysBytes(byte[] tsBytes, int fileNum, long offset, boolean isSort) {
        return DBUtil.dataBase.leaftimekey_from_tskey(tsBytes, fileNum, offset, isSort);
    }
    static int c = 0;
    public static void putSaxes(ArrayList<LeafTimeKey> leafTimeKeys) {

        for (LeafTimeKey leafTimeKey : leafTimeKeys) {
//            System.out.println(Arrays.toString(sax.getLeafTimeKeys()));
            DBUtil.dataBase.put(leafTimeKey.getLeafTimeKeys());


        }
    }
    public static void putSaxesBytes(byte[] leafTimeKeysBytes) {
        DBUtil.dataBase.put(leafTimeKeysBytes);
//        System.out.println("sax存储完成");
    }
}
