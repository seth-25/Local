package com.local.search;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.domain.Pair;
import com.local.domain.Parameters;
import com.local.domain.Version;
import com.local.util.CacheUtil;
import com.local.util.DBUtil;
import com.local.util.PrintUtil;
import com.local.util.SaxTUtil;
import com.local.version.VersionAction;
import com.local.version.VersionUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class SearchActionBuffer {
    public static ByteBuffer searchRtree(RTree<String, Rectangle> rTree, long startTime, long endTime, byte[] minSaxT, byte[] maxSaxT) {
        Iterable<Entry<String, Rectangle>> results;
        if (Parameters.hasTimeStamp > 0) {
            results = rTree.search(
                    Geometries.rectangle(VersionUtil.saxT2Double(minSaxT), (double) startTime, VersionUtil.saxT2Double(maxSaxT), (double) endTime)
            ).toBlocking().toIterable();
        }
        else {
            results = rTree.search(
                    Geometries.rectangle(VersionUtil.saxT2Double(minSaxT), 0, VersionUtil.saxT2Double(maxSaxT), 0)
            ).toBlocking().toIterable();
        }


        ArrayList<Long> sstableNumList = new ArrayList<>();

        for (Entry<String, Rectangle> result : results) {
            String value = result.value();
            byte[] nodeMinSaxT = value.substring(0, Parameters.saxTSize).getBytes(StandardCharsets.ISO_8859_1);
            byte[] nodeMaxSaxT = value.substring(Parameters.saxTSize, 2 * Parameters.saxTSize).getBytes(StandardCharsets.ISO_8859_1);
            if (SaxTUtil.compareSaxT(nodeMinSaxT, maxSaxT) <= 0 && SaxTUtil.compareSaxT(nodeMaxSaxT, minSaxT) >= 0) {
                sstableNumList.add(Long.valueOf(value.substring(2 * Parameters.saxTSize)));
            }
        }
        ByteBuffer sstableNumBuffer = ByteBuffer.allocateDirect(8 * sstableNumList.size()).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < sstableNumList.size(); i ++ ) {
            sstableNumBuffer.putLong(sstableNumList.get(i));
        }
        return sstableNumBuffer;
    }

    // 近似查询,除了ares还返回sstableNum
    public static Pair<Pair<Integer, ByteBuffer>, ByteBuffer> getAresFromDB(boolean isUseAm, long startTime, long endTime, byte[] saxTData, ByteBuffer aQuery, int d,
                                       RTree<String, Rectangle> rTree, int amVersionID, int stVersionID, int k) {
        ByteBuffer sstableNumBuffer;
        if (d == Parameters.bitCardinality) {
            sstableNumBuffer = searchRtree(rTree, startTime, endTime, saxTData, saxTData);
        }
        else {
            byte[] minSaxT = SaxTUtil.makeMinSaxT(saxTData, d);
            byte[] maxSaxT = SaxTUtil.makeMaxSaxT(saxTData, d);
            sstableNumBuffer = searchRtree(rTree, startTime, endTime, minSaxT, maxSaxT);
        }
//        PrintUtil.print("r树结果: sstableNum：" + Arrays.toString(sstableNum));

        // Get返回若干个ares,ares的最后有一个4字节的id,用于标记近似查的是当前am版本中的哪个表(一个am版本有多个表并行维护不同的saxt树),用于精准查询的appro_res(去重)
        ByteBuffer approRes = ByteBuffer.allocateDirect(k * Parameters.aresSize + 4);   // 空的ByteBuffer给C写
        ByteBuffer infoBuffer = ByteBuffer.allocateDirect(Parameters.tsSize + 24 + Parameters.infoMaxPSize * 8); // 空的ByteBuffer给C写
        int numAres = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID,
                sstableNumBuffer.capacity() / 8, sstableNumBuffer, approRes, infoBuffer);
//        PrintUtil.print("近似查询结果长度" + approRes.capacity());
        return new Pair<>(new Pair<>(numAres, approRes), sstableNumBuffer);
    }


    public static ByteBuffer searchTs(ByteBuffer searchTsBuffer, long startTime, long endTime, int k) {
        PrintUtil.print("近似查询===");
        boolean isUseAm = true; // saxT范围 是否在个该机器上
//        byte[] saxTData = new byte[Parameters.saxTSize];
        ByteBuffer saxTBuffer = ByteBuffer.allocateDirect(Parameters.saxTSize);
//        float[] paa = new float[Parameters.paaNum];
        ByteBuffer paaBuffer = ByteBuffer.allocateDirect(Parameters.paaNum * 4);
        PrintUtil.print("searchBuffer " + searchTsBuffer.toString());
        PrintUtil.print("saxT: "  + saxTBuffer.toString());
        DBUtil.dataBase.paa_saxt_from_ts_buffer(searchTsBuffer, saxTBuffer, paaBuffer);
        PrintUtil.print("saxT: "  + saxTBuffer.toString());
        ByteBuffer aQuery;
        if (Parameters.hasTimeStamp > 0) {
            aQuery = SearchUtil.makeAQuery(searchTsBuffer, startTime, endTime, k, paaBuffer, saxTBuffer);
        }
        else {
            aQuery = SearchUtil.makeAQuery(searchTsBuffer, k, paaBuffer, saxTBuffer);
        }


        // 获取版本
        RTree<String, Rectangle> rTree;
        int amVersionID, stVersionID;
        Version version;
        synchronized(VersionAction.class) {
            version = CacheUtil.curVersion;
            Pair<Integer, Integer> verPair = version.getWorkerVersions().get(Parameters.hostName);
            if (verPair == null) {
                throw new RuntimeException("当前版本为空");
            }
            amVersionID = verPair.getKey();    // 查询到来时该worker的版本号
            stVersionID = verPair.getValue();
            rTree = version.getrTree();
            VersionAction.refVersion(version);
        }
        PrintUtil.print("近似查询版本: amVersionID:" + amVersionID + " stVersionID:" + stVersionID);


        // 近似查询
        byte[] saxTData = new byte[saxTBuffer.remaining()];
        saxTBuffer.get(saxTData);
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1,增大查询范围
        Pair<Integer, ByteBuffer> aresPair = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                rTree, amVersionID, stVersionID, k).getKey();
        int numAres = aresPair.getKey();
        ByteBuffer ares = aresPair.getValue();
        while(numAres < k && d > 0) { // 查询结果不够k个
            d --;
            aresPair = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                    rTree, amVersionID, stVersionID, k).getKey();
            ares = aresPair.getValue();
            numAres = aresPair.getKey();
        }

        // 释放版本
        VersionAction.unRefVersion(version);

        return ares;
    }


    public static ByteBuffer searchExactTs(ByteBuffer searchTsBuffer, long startTime, long endTime, int k) {
        PrintUtil.print("精确查询===");
        boolean isUseAm = true; // saxT范围 是否在个该机器上
//        byte[] saxTData = new byte[Parameters.saxTSize];
        ByteBuffer saxTBuffer = ByteBuffer.allocateDirect(Parameters.saxTSize);
//        float[] paa = new float[Parameters.paaNum];
        ByteBuffer paaBuffer = ByteBuffer.allocateDirect(Parameters.paaNum * 4);
        PrintUtil.print("searchBuffer " + searchTsBuffer.toString());
        PrintUtil.print("saxT: "  + saxTBuffer.toString());
        DBUtil.dataBase.paa_saxt_from_ts_buffer(searchTsBuffer, saxTBuffer, paaBuffer);
        PrintUtil.print("saxT: "  + saxTBuffer.toString());
        ByteBuffer aQuery;
        if (Parameters.hasTimeStamp > 0) {
            aQuery = SearchUtil.makeAQuery(searchTsBuffer, startTime, endTime, k, paaBuffer, saxTBuffer);
        }
        else {
            aQuery = SearchUtil.makeAQuery(searchTsBuffer, k, paaBuffer, saxTBuffer);
        }

        // 获取版本
        RTree<String, Rectangle> rTree;
        int amVersionID, stVersionID;
        Version version;
        synchronized(VersionAction.class) {
            version = CacheUtil.curVersion;
            Pair<Integer, Integer> verPair = version.getWorkerVersions().get(Parameters.hostName);
            if (verPair == null) {
                throw new RuntimeException("当前版本为空");
            }
            amVersionID = verPair.getKey();    // 查询到来时该worker的版本号
            stVersionID = verPair.getValue();
            rTree = version.getrTree();
            VersionAction.refVersion(version);
        }

        // 近似查询
        byte[] saxTData = new byte[saxTBuffer.remaining()];
        saxTBuffer.get(saxTData);
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1
        Pair<Pair<Integer, ByteBuffer>, ByteBuffer> aresAndSSNum = getAresFromDB(isUseAm, startTime, endTime, saxTData,
                aQuery, d, rTree, amVersionID, stVersionID, k);
        Pair<Integer, ByteBuffer> aresPair = aresAndSSNum.getKey();
        ByteBuffer approSSTableNum = aresAndSSNum.getValue();
        int numAres = aresPair.getKey();
        ByteBuffer approRes = aresPair.getValue();
        while((numAres - 4) / Parameters.aresSize < k && d > 0) { // 查询结果不够k个
            d --;
            aresPair = getAresFromDB(isUseAm, startTime, endTime, saxTData,
                    aQuery, d, rTree, amVersionID, stVersionID, k).getKey();
            approRes = aresPair.getValue();
            numAres = aresPair.getKey();
        }

//        // todo
//        System.out.println("判断近似结果正确");
//        for (int i = 0; i < approRes.length - 4; i += Parameters.aresSize) {
//            byte[] tsBytes = new byte[Parameters.timeSeriesDataSize];
//            System.arraycopy(approRes, i, tsBytes, 0, Parameters.timeSeriesDataSize);
//            byte[] floatBytes = new byte[4];
//            System.arraycopy(approRes, i + Parameters.tsSize, floatBytes, 0, 4);
//            byte[] pBytes = new byte[8];
//            System.arraycopy(approRes, i + Parameters.tsSize + 8, pBytes, 0, 8);
//            long p = SearchUtil.bytesToLong(pBytes);
//            if (bitmap.get(p) != null) {
//                System.out.println(" 近似p有重复！！！ " + p);
//            }
//            bitmap.put(p, 1L);
//            int p_hash = (int) (p >> 56);   // 文件名
//            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
//            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
//            byte[] ts;
//            synchronized (reader) {
//                ts = reader.readTsNewByte(offset);
//            }
//            for (int j = 0; j < Parameters.tsSize; j ++ ) {
//                if (ts[j] != tsBytes[j]) {
//                    System.out.println("错误！！！！！！！！！！！！！");
//
//                    System.out.println(Arrays.toString(ts).substring(0,100));
//                    System.out.println(Arrays.toString(tsBytes).substring(0,100));
//                    break;
//                }
//                if (SearchUtil.bytesToFloat(floatBytes) != DBUtil.dataBase.dist_ts(ts, searchTsBytes)) {
//                    System.out.println("距离错误！！！！！！！！！！！！！");
//
//                    System.out.println(Arrays.toString(ts).substring(0,100));
//                    System.out.println(Arrays.toString(tsBytes).substring(0,100));
//                }
//            }
//        }

        // 精确
        Iterable<Entry<String, Rectangle>> results;
        if (Parameters.hasTimeStamp > 0) {
            results = rTree.search(  // 所有saxT范围
                    Geometries.rectangle(-Double.MAX_VALUE, (double) startTime, Double.MAX_VALUE, (double) endTime)
            ).toBlocking().toIterable();
        }
        else {
            results = rTree.search(  // 所有saxT范围
                    Geometries.rectangle(-Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE)
            ).toBlocking().toIterable();
        }

        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String value = result.value();
            sstableNumList.add(Long.valueOf(value.substring(2 * Parameters.saxTSize)));
        }
        ByteBuffer sstableNumBuffer = ByteBuffer.allocateDirect(8 * sstableNumList.size()).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < sstableNumList.size(); i ++ ) {
            sstableNumBuffer.putLong(sstableNumList.get(i));
        }


        PrintUtil.print("精确查询版本: amVersionID:" + amVersionID + "\tstVersionID:" + stVersionID);
//        PrintUtil.print("精确查询 r树结果:" + Arrays.toString(sstableNum) + "\t近似查询 r树结果:" + Arrays.toString(approSSTableNum));
//        PrintUtil.print("aQuery长度 " + aQuery.length + "\t近似查询长度 " + approRes.length);

        ByteBuffer exactRes = ByteBuffer.allocateDirect(k * Parameters.aresExactSize);   // 空的ByteBuffer给C写
        ByteBuffer infoBuffer = ByteBuffer.allocateDirect(Parameters.tsSize + 24 + Parameters.infoMaxPSize * 8); // 空的ByteBuffer给C写
        int numExactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID,
                sstableNumList.size(), sstableNumBuffer, numAres, approRes,
                approSSTableNum.capacity() / 8, approSSTableNum, exactRes, infoBuffer);
//        PrintUtil.print("精确查询结果长度" + exactRes.length);

        // 释放版本
        VersionAction.unRefVersion(version);

        return exactRes;
    }
}
