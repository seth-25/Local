package com.local.search;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.domain.Parameters;
import com.local.util.*;
import com.local.version.VersionAction;
import com.local.version.VersionUtil;
import javafx.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class SearchAction {

    
    
    public static byte[] searchNearlyTs(byte[] info) {  // 查找最近的至多k个ts
        System.out.println("info长度" + info.length);
        System.out.println("查询相近");
        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        if (Parameters.hasTimeStamp > 0) {
            SearchUtil.analysisSearchSend(info, aQuery);
        }
        else {
            SearchUtil.analysisSearchSendHasNotTime(info, aQuery);
        }
        aQuery.sortPList();

        List<Pair<byte[], Float>> searchTS = new ArrayList<>();
        for (Long p : aQuery.pList) {
            int p_hash = (int)(p >> 56);
            long offset = p.longValue() & 0x00ffffffffffffffL;

//            FileChannelReader reader = CacheUtil.fileChannelReaderMap.get(p_hash);
            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
            byte[] tskey = null;
            synchronized (reader) {
                tskey = reader.readTs(offset);
            }

            if (Parameters.hasTimeStamp == 1) {
                long timestamps = TsUtil.bytesToLong(tskey, Parameters.timeSeriesDataSize);
                if (timestamps >= aQuery.startTime && timestamps <= aQuery.endTime) {
                    searchTS.add(new Pair<>(tskey, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, tskey)));
                }
            }
            else {
                searchTS.add(new Pair<>(tskey, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, tskey)));
            }
        }

        searchTS.sort(new Comparator<Pair<byte[], Float>>() {
            @Override
            public int compare(Pair<byte[], Float> o1, Pair<byte[], Float> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        int cnt = 0;
        byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresSize];
        for (Pair<byte[], Float> tsPair: searchTS) {
            float dis = tsPair.getValue();
            System.out.println(dis);
            if (cnt >= aQuery.needNum) {
                if (dis > aQuery.topDist || cnt >= aQuery.k) break;
            }

            System.arraycopy(tsPair.getKey(), 0, tmp, cnt * Parameters.aresSize, Parameters.tsSize);
            System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.aresSize + Parameters.tsSize, 4);
            cnt ++;
        }
        System.out.println("访问原始时间序列个数" + aQuery.pList.size() + " " + "返回原始时间序列个数cnt:" + cnt);
        byte[] res = new byte[cnt * Parameters.aresSize];  // res: cnt个ares
        System.arraycopy(tmp, 0, res, 0, cnt * Parameters.aresSize);
        return res;
    }


    public static long[] searchRtree(RTree<String, Rectangle> rTree, long startTime, long endTime, byte[] minSaxT, byte[] maxSaxT) {
        Iterable<Entry<String, Rectangle>> results = rTree.search(
                Geometries.rectangle(VersionUtil.saxT2Double(minSaxT), (double) startTime, VersionUtil.saxT2Double(maxSaxT), (double) endTime)
        ).toBlocking().toIterable();

        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String[] str = result.value().split(":");
            if (SaxTUtil.compareSaxT(str[0].getBytes(StandardCharsets.ISO_8859_1), maxSaxT) <= 0 && SaxTUtil.compareSaxT(str[1].getBytes(StandardCharsets.ISO_8859_1), minSaxT) >= 0) {
                sstableNumList.add(Long.valueOf(str[2]));
            }
        }
        return sstableNumList.stream().mapToLong(num -> num).toArray();
    }

    // 近似查询
    public static byte[] getTsFromDB(boolean isUseAm, long startTime, long endTime, byte[] saxTData, byte[] aQuery, int d) {
        byte[] ares;
        RTree<String, Rectangle> rTree;
        int amVersionID, stVersionID;
        synchronized(VersionAction.class) {
            Pair<Integer, Integer> verPair = CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName);
            if (verPair == null) {
                throw new RuntimeException("当前版本为空");
            }
            amVersionID = verPair.getKey();    // 查询到来时该worker的版本号
            stVersionID = verPair.getValue();
            rTree = CacheUtil.curVersion.getrTree();
            VersionAction.refCurVersion();
        }

        long[] sstableNum;
        if (d == Parameters.bitCardinality) {
            sstableNum = searchRtree(rTree, startTime, endTime, saxTData, saxTData);
        }
        else {
            byte[] minSaxT = SaxTUtil.makeMinSaxT(saxTData, d);
            byte[] maxSaxT = SaxTUtil.makeMaxSaxT(saxTData, d);
            sstableNum = searchRtree(rTree, startTime, endTime, minSaxT, maxSaxT);
        }

        System.out.println("amVersionID:" + amVersionID + " stVersionID:" + stVersionID);
        System.out.println("sstableNum：" + Arrays.toString(sstableNum));

//        System.out.println(Arrays.toString(aQuery));
        ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);

        VersionAction.unRefCurVersion();
        System.out.println("近似长度" + ares.length);
        return ares;
    }


    public static byte[] searchTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaSize];
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxTData, paa);
        System.out.println("saxT: "  + Arrays.toString(saxTData));
        System.out.println("saxT的浮点值: " + VersionUtil.saxT2Double(saxTData));
        byte[] aQuery;
        if (Parameters.hasTimeStamp > 0) {
           aQuery = SearchUtil.makeAQuery(searchTsBytes, startTime, endTime, k, paa, saxTData);
        }
        else {
           aQuery = SearchUtil.makeAQuery(searchTsBytes, k, paa, saxTData);
        }

        int d = Parameters.bitCardinality;  // 相聚度,开始为离散化个数,找不到k个则-1
        byte[] res = getTsFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d);

        while(res.length / Parameters.tsSize < k && d > 0) { // 查询结果不够k个
            d --;
            res = getTsFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d);
        }
        return res;
    }

    public static byte[] searchExactTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaSize];
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxTData, paa);
        System.out.println("saxT: "  + Arrays.toString(saxTData));
        System.out.println("saxT的浮点值: " + VersionUtil.saxT2Double(saxTData));
        byte[] aQuery;
        if (Parameters.hasTimeStamp > 0) {
            aQuery = SearchUtil.makeAQuery(searchTsBytes, startTime, endTime, k, paa, saxTData);
        }
        else {
            aQuery = SearchUtil.makeAQuery(searchTsBytes, k, paa, saxTData);
        }


        // 近似
        byte[] ares;
        int d = Parameters.bitCardinality;  // 相聚度,开始为离散化个数,找不到k个则-1
        ares = getTsFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d);

        while(ares.length / Parameters.tsSize < k && d > 0) { // 查询结果不够k个
            d --;
            ares = getTsFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d);
        }


        // 精确
        byte[] exactRes;
        RTree<String, Rectangle> rTree;
        int amVersionID, stVersionID;
        synchronized(VersionAction.class) {
            Pair<Integer, Integer> verPair = CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName);
            if (verPair == null) {
                throw new RuntimeException("当前版本为空");
            }
            amVersionID = verPair.getKey();    // 查询到来时该worker的版本号
            stVersionID = verPair.getValue();
            rTree = CacheUtil.curVersion.getrTree();
            VersionAction.refCurVersion();
        }

        Iterable<Entry<String, Rectangle>> results = rTree.search(  // 所有saxT范围
                Geometries.rectangle(-Double.MAX_VALUE, (double) startTime, Double.MAX_VALUE, (double) endTime)
        ).toBlocking().toIterable();

        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String[] str = result.value().split(":");
            if (SaxTUtil.compareSaxT(str[0].getBytes(StandardCharsets.ISO_8859_1), saxTData) <= 0 && SaxTUtil.compareSaxT(str[1].getBytes(StandardCharsets.ISO_8859_1), saxTData) >= 0) {
                sstableNumList.add(Long.valueOf(str[2]));
            }
        }
        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();

        System.out.println("amVersionID:" + amVersionID + " stVersionID:" + stVersionID);
        System.out.println("sstableNum：" + Arrays.toString(sstableNum));

        exactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID, sstableNum, ares);
        System.out.println("结果长度" + exactRes.length);

        VersionAction.unRefCurVersion();

        return exactRes;
    }


}
