package com.local.insert;

import com.local.domain.Parameters;
import com.local.domain.LeafTimeKey;
import com.local.util.*;

import java.util.ArrayList;

public class InsertAction {


//    public static ArrayList<LeafTimeKey> getSaxes(byte[] tsBytes, int fileNum, long offset) {
//        byte[] leafTimeKeysBytes = DBUtil.dataBase.leaftimekey_from_tskey(tsBytes, fileNum, offset, false);
//        ArrayList<LeafTimeKey> leafTimeKeys = new ArrayList<>();
//        for (int i = 0; i < leafTimeKeysBytes.length / Parameters.LeafTimeKeysSize; i ++ ) {
//            byte[] data = new byte[Parameters.saxTSize];
//            byte[] p_offset = new byte[Parameters.pointerSize - 1];
//            byte[] timeStamp = new byte[Parameters.timeStampSize];
//            System.arraycopy(leafTimeKeysBytes, Parameters.LeafTimeKeysSize * i, p_offset, 0, p_offset.length);
//            byte p_hash = leafTimeKeysBytes[Parameters.LeafTimeKeysSize * i + p_offset.length];
//            System.arraycopy(leafTimeKeysBytes, Parameters.LeafTimeKeysSize * i + 1 + p_offset.length, data, 0, data.length);
//            System.arraycopy(leafTimeKeysBytes, Parameters.LeafTimeKeysSize * i + data.length + 1 + p_offset.length, timeStamp, 0, timeStamp.length);
//            LeafTimeKey leafTimeKey = new LeafTimeKey(data, p_hash, p_offset, timeStamp);
//            leafTimeKeys.add(leafTimeKey);
//        }
//        return leafTimeKeys;
//    }

    public static byte[] getLeafTimeKeysBytes(byte[] tsBytes, int fileNum, long offset) {
        return DBUtil.dataBase.leaftimekey_from_tskey(tsBytes, fileNum, offset, false);
    }
    public static byte[] getLeafTimeKeysBytes(byte[] tsBytes, int fileNum, long offset, boolean isSort) {
        return DBUtil.dataBase.leaftimekey_from_tskey(tsBytes, fileNum, offset, isSort);
    }
//    public static void putSaxes(ArrayList<LeafTimeKey> leafTimeKeys) {
//        for (LeafTimeKey leafTimeKey : leafTimeKeys) {
////            System.out.println(Arrays.toString(sax.getLeafTimeKeys()));
//            DBUtil.dataBase.put(leafTimeKey.getLeafTimeKeys());
//        }
//    }
    public static void putLeafTimeKeysBytes(byte[] leafTimeKeysBytes) {
        DBUtil.dataBase.put(leafTimeKeysBytes);
//        CacheUtil.insertThreadPool.execute(new Runnable() {
//            @Override
//            public void run() {
//                DBUtil.dataBase.put(leafTimeKeysBytes);
//            }
//        });
//        System.out.println("sax存储完成");
    }
}
