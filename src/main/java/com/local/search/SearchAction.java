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
        System.out.println("查询相近");
        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        SearchUtil.analysisSearchSend(info, aQuery);
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

            long timestamps = TsUtil.bytesToLong(tskey, Parameters.timeSeriesDataSize);
            if (timestamps >= aQuery.startTime && timestamps <= aQuery.endTime) {
                searchTS.add(new Pair<>(tskey, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, tskey)));
            }
        }

        searchTS.sort(new Comparator<Pair<byte[], Float>>() {
            @Override
            public int compare(Pair<byte[], Float> o1, Pair<byte[], Float> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        //  ares
        //  ts 256*4 time 8
        //  float dist 4 最后4位为空
        //  1040
        int cnt = 0;
        byte[] tmp = new byte[aQuery.pList.size() * 1040];
        for (Pair<byte[], Float> tsPair: searchTS) {
            float dis = tsPair.getValue();
            System.out.println(dis);
            if (cnt >= aQuery.needNum) {
                if (dis > aQuery.topDist || cnt >= aQuery.k) break;
            }

            System.arraycopy(tsPair.getKey(), 0, tmp, cnt * 1040, Parameters.tsSize);
            System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * 1040 + Parameters.tsSize, 4);
            cnt ++;
        }
        System.out.println("cnt:"+cnt);
        byte[] res = new byte[cnt * 1040];
        System.arraycopy(tmp, 0, res, 0, cnt * 1040);
        return res;
    }


    public static byte[] searchTs(byte[] searchTsBytes, long startTime, long endTime, int k) {

        boolean isUseAm = true; // sax范围 是否在个该机器上
        byte[] saxData = new byte[Parameters.saxSize];
        float[] paa = new float[Parameters.paaSize];
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxData, paa);
        System.out.println("sax: "  + Arrays.toString(saxData));
        System.out.println("sax的浮点值: " + VersionUtil.saxToDouble(saxData));
        byte[] aQuery = SearchUtil.makeAQuery(searchTsBytes, startTime, endTime, k, paa, saxData);

        byte[] ares = null;
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

        Iterable<Entry<String, Rectangle>> results = rTree.search(
                Geometries.rectangle(VersionUtil.saxToDouble(saxData), (double) startTime,
                        VersionUtil.saxToDouble(saxData), (double) endTime)
        ).toBlocking().toIterable();

        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String[] str = result.value().split(":");
            if (SaxUtil.compareSax(str[0].getBytes(StandardCharsets.ISO_8859_1), saxData) <= 0 && SaxUtil.compareSax(str[1].getBytes(StandardCharsets.ISO_8859_1), saxData) >= 0) {
                sstableNumList.add(Long.valueOf(str[2]));
            }
        }
        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();

        System.out.println("amVersionID:" + amVersionID + " stVersionID:" + stVersionID);
        System.out.println("sstableNum：" + Arrays.toString(sstableNum));


        System.out.println(Arrays.toString(aQuery));
        ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);

        VersionAction.unRefCurVersion();

        return ares;
    }

    public static byte[] searchExactTs(byte[] searchTsBytes, long startTime, long endTime, int k) {

        boolean isUseAm = true; // sax范围 是否在个该机器上
        byte[] saxData = new byte[Parameters.saxSize];
        float[] paa = new float[32];
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxData, paa);
        System.out.println("sax: "  + Arrays.toString(saxData));
        System.out.println("sax的浮点值: " + VersionUtil.saxToDouble(saxData));
        byte[] aQuery = SearchUtil.makeAQuery(searchTsBytes, startTime, endTime, k, paa, saxData);

        byte[] ares = null;
        byte[] exactRes = null;
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
        // 近似
        Iterable<Entry<String, Rectangle>> resultsNearly = rTree.search(
                Geometries.rectangle(VersionUtil.saxToDouble(saxData), (double) startTime, VersionUtil.saxToDouble(saxData), (double) endTime)
        ).toBlocking().toIterable();
        ArrayList<Long> sstableNumListNearly = new ArrayList<>();
        for (Entry<String, Rectangle> result : resultsNearly) {
            String[] str = result.value().split(":");
            if (SaxUtil.compareSax(str[0].getBytes(StandardCharsets.ISO_8859_1), saxData) <= 0 && SaxUtil.compareSax(str[1].getBytes(StandardCharsets.ISO_8859_1), saxData) >= 0) {
                sstableNumListNearly.add(Long.valueOf(str[2]));
            }
        }
        long[] sstableNumNearly = sstableNumListNearly.stream().mapToLong(num -> num).toArray();
        ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNumNearly);


        // 精确
        Iterable<Entry<String, Rectangle>> results = rTree.search(  // 所有sax范围
                Geometries.rectangle(-Double.MAX_VALUE, (double) startTime, Double.MAX_VALUE, (double) endTime)
        ).toBlocking().toIterable();

        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String[] str = result.value().split(":");
            if (SaxUtil.compareSax(str[0].getBytes(StandardCharsets.ISO_8859_1), saxData) <= 0 && SaxUtil.compareSax(str[1].getBytes(StandardCharsets.ISO_8859_1), saxData) >= 0) {
                sstableNumList.add(Long.valueOf(str[2]));
            }
        }

        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();

        System.out.println("amVersionID:" + amVersionID + " stVersionID:" + stVersionID);
        System.out.println("sstableNum：" + Arrays.toString(sstableNum));

        exactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID, sstableNum, ares);

        VersionAction.unRefCurVersion();

        return exactRes;
    }


}
