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

    /**
     访问原始时间序列,查找最近的至多k个ts
     近似查询返回的ares包含p,精确查询返回的ares不包含p
    **/
    static class OriTs {
        byte[] ts = new byte[Parameters.tsSize];    // 时间序列+时间戳(如果有)
        Float dis;
        byte[] p;
        public OriTs(byte[] ts, float dis, byte[] p) {
            this.ts = ts;
            this.dis = dis;
            this.p = p;
        }
    }
    public static byte[] searchOriTs(byte[] info, boolean isExact) {
        System.out.println("查询原始时间序列 info长度" + info.length);
        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        if (Parameters.hasTimeStamp > 0) {
            SearchUtil.analysisSearchSend(info, aQuery);
        } else {
            SearchUtil.analysisSearchSendHasNotTime(info, aQuery);
        }
        aQuery.sortPList();

//        List<Pair<byte[], Float>> nearlyTsList = new ArrayList<>();
        List<OriTs> nearlyTsList = new ArrayList<>();
        for (int i = 0; i < aQuery.pList.size(); i ++ ) {
            Long p = aQuery.pList.get(i);
            int p_hash = (int) (p >> 56);   // 文件名
            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置

//            FileChannelReader reader = CacheUtil.fileChannelReaderMap.get(p_hash);
            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
            byte[] ts;
            synchronized (reader) {
                ts = reader.readTs(offset);
            }
            if (Parameters.hasTimeStamp == 1) { // 有时间戳才需要判断原始时间序列的时间范围

                long timestamps = TsUtil.bytesToLong(ts, Parameters.timeSeriesDataSize);
                if (timestamps >= aQuery.startTime && timestamps <= aQuery.endTime) {
                    OriTs oriTs = new OriTs(ts, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts),  aQuery.pBytesList.get(i));
                    nearlyTsList.add(oriTs);
                }
            } else {
                OriTs oriTs = new OriTs(ts, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts),  aQuery.pBytesList.get(i));
                nearlyTsList.add(oriTs);
            }

        }

        nearlyTsList.sort(new Comparator<OriTs>() {
            @Override
            public int compare(OriTs o1, OriTs o2) {
                return o1.dis.compareTo(o2.dis);
            }
        });

        int cnt = 0;
        if (isExact) {
            // 返回至多k个ares_exact(不含p)
            // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
            // ares_exact(没时间戳): ts 256*4, float dist 4
            byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresExactSize];
            for (OriTs oriTs : nearlyTsList) {
                float dis = oriTs.dis;
                System.out.println("距离:" + dis + " topDis:" + aQuery.topDist);
                if (dis < aQuery.topDist || cnt < aQuery.needNum) {
                    System.arraycopy(oriTs.ts, 0, tmp, cnt * Parameters.aresExactSize, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.aresExactSize + Parameters.tsSize, 4);
                    cnt++;
                }
                else {
                    break;
                }


            }
            System.out.println("需要个数" + aQuery.needNum);
            System.out.println("精确查询:访问原始时间序列个数" + aQuery.pList.size() + " " + "返回原始时间序列个数cnt:" + cnt);
            byte[] aresExact = new byte[cnt * Parameters.aresExactSize];  // aresExact: cnt个ares
            System.arraycopy(tmp, 0, aresExact, 0, cnt * Parameters.aresExactSize);
            return aresExact;
        }
        else {
            // 返回至多k个ares(含p)
            // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
            byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresSize];
            for (OriTs oriTs : nearlyTsList) {
                float dis = oriTs.dis;
                System.out.println("距离:" + dis + " topDis:" + aQuery.topDist);
                if (cnt < aQuery.k && (dis < aQuery.topDist || cnt < aQuery.needNum)) {
                    System.arraycopy(oriTs.ts, 0, tmp, cnt * Parameters.aresSize, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.aresSize + Parameters.tsSize, 4);
                    System.arraycopy(oriTs.p, 0, tmp, cnt * Parameters.aresSize + Parameters.tsSize + 4, Parameters.pointerSize);
                    cnt++;
                }
                else {
                    break;
                }
            }

            System.out.println("近似查询:需要个数:"+ aQuery.needNum + "访问原始时间序列个数:" + aQuery.pList.size() + " " + "返回原始时间序列个数cnt:" + cnt);
            byte[] ares = new byte[cnt * Parameters.aresSize];  // aresExact: cnt个ares
            System.arraycopy(tmp, 0, ares, 0, cnt * Parameters.aresSize);
            return ares;
        }

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

    // 近似查询,返回若干ares
    public static byte[] getAresFromDB(boolean isUseAm, long startTime, long endTime, byte[] saxTData, byte[] aQuery, int d,
                                       RTree<String, Rectangle> rTree, int amVersionID, int stVersionID) {
        long[] sstableNum;
        if (d == Parameters.bitCardinality) {
            sstableNum = searchRtree(rTree, startTime, endTime, saxTData, saxTData);
        }
        else {
            byte[] minSaxT = SaxTUtil.makeMinSaxT(saxTData, d);
            byte[] maxSaxT = SaxTUtil.makeMaxSaxT(saxTData, d);
            sstableNum = searchRtree(rTree, startTime, endTime, minSaxT, maxSaxT);
        }
        System.out.println("r树结果: sstableNum：" + Arrays.toString(sstableNum));

        byte[] ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);

        VersionAction.unRefCurVersion();
        System.out.println("近似查询结果长度" + ares.length);
        return ares;
    }

    // 近似查询,除了
    public static Pair<byte[], long[]> getAresAndSSTableFromDB(boolean isUseAm, long startTime, long endTime, byte[] saxTData, byte[] aQuery, int d,
                                                               RTree<String, Rectangle> rTree, int amVersionID, int stVersionID) {
        long[] sstableNum;
        if (d == Parameters.bitCardinality) {
            sstableNum = searchRtree(rTree, startTime, endTime, saxTData, saxTData);
        }
        else {
            byte[] minSaxT = SaxTUtil.makeMinSaxT(saxTData, d);
            byte[] maxSaxT = SaxTUtil.makeMaxSaxT(saxTData, d);
            sstableNum = searchRtree(rTree, startTime, endTime, minSaxT, maxSaxT);
        }
        System.out.println("r树结果: sstableNum：" + Arrays.toString(sstableNum));

        byte[] ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);

        VersionAction.unRefCurVersion();
        System.out.println("近似查询结果长度" + ares.length);
        return new Pair<>(ares, sstableNum);
    }


    public static byte[] searchTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaNum];
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


        // 获取版本
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
        System.out.println("近似查询版本: amVersionID:" + amVersionID + " stVersionID:" + stVersionID);


        // 近似查询
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1,增大查询范围
        byte[] ares = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                rTree, amVersionID, stVersionID);

        while((ares.length - 4) / Parameters.aresSize < k && d > 0) { // 查询结果不够k个
            d --;
            ares = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                rTree, amVersionID, stVersionID);
        }

        // 释放版本
        VersionAction.unRefCurVersion();

        return ares;
    }

    public static byte[] searchExactTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaNum];
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


        // 获取版本
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
        System.out.println("精确查询版本: amVersionID:" + amVersionID + " stVersionID:" + stVersionID);

        // 近似查询
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1

        Pair<byte[], long[]> aresAndSSNum = getAresAndSSTableFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                                                    rTree, amVersionID, stVersionID);
        byte[] approRes = aresAndSSNum.getKey();
        long[] approSSTableNum = aresAndSSNum.getValue();

        while((approRes.length - 4) / Parameters.aresSize < k && d > 0) { // 查询结果不够k个
            d --;
            approRes = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                        rTree, amVersionID, stVersionID);
        }


        // 精确
        byte[] exactRes;

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


        System.out.println("r树结果: sstableNum：" + Arrays.toString(sstableNum));

        System.out.println(amVersionID  + " " + stVersionID + " " + Arrays.toString(sstableNum) + " " + Arrays.toString(approSSTableNum));
        System.out.println("aQuery长度 " + aQuery.length + " " + "近似查询长度 " + approRes.length);

        exactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID, sstableNum, approRes, approSSTableNum);
        System.out.println("精确查询结果长度" + exactRes.length);

        // 释放版本
        VersionAction.unRefCurVersion();

        return exactRes;
    }


}
