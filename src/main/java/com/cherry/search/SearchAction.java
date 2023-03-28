package com.cherry.search;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.cherry.Main;
import com.cherry.domain.Parameters;
import com.cherry.domain.Version;
import com.cherry.util.*;
import com.cherry.version.VersionAction;
import com.cherry.version.VersionUtil;
import com.cherry.domain.Pair;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class SearchAction {

    /**
     访问原始时间序列,查找最近的至多k个ts
     近似查询返回的ares包含p,精确查询返回的ares不包含p
    **/
    static class RawTs {
        byte[] ts;    // 时间序列+时间戳(如果有)
        Float dis;
        byte[] p;
        public RawTs(byte[] ts, float dis, byte[] p) {
            this.ts = ts;
            this.dis = dis;
            this.p = p;
        }
    }

    public static byte[] searchRawTs(byte[] info, boolean isExact) {
        long searchRawTsTimeStart = System.currentTimeMillis(); // todo
        PrintUtil.print("查询原始时间序列 info长度" + info.length + " " + Thread.currentThread().getName() + " isExact " + isExact);  // todo
        long readTime = 0;   // todo
        long readLockTime = 0;   // todo
        long makePTime = 0;   // todo
        long forTime;
        long sortTime;
        long disTime = 0;

        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        if (Parameters.hasTimeStamp > 0) {
            SearchUtil.analysisInfo(info, aQuery);
        } else {
            SearchUtil.analysisInfoNoTime(info, aQuery);
        }
        Arrays.sort(aQuery.pArray);

        long forTimeStart = System.currentTimeMillis();   // todo
        List<RawTs> nearlyTsList = new ArrayList<>();
        for (int i = 0; i < aQuery.pArray.length; i ++ ) {
            long makeTimeStart = System.currentTimeMillis();    // todo
            long p = aQuery.pArray[i];

//            if (isExact) {
//                Long a = bitmap.get(p);
//                if (a != null && a == 1) {
//                    System.out.println("精确有近似 " + p);
//                }
//            }

            int p_hash = (int) (p >> 56);   // 文件名
            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
            MappedFileReaderBuffer reader = CacheUtil.mappedFileReaderMapBuffer.get(p_hash);
            byte[] ts;
            makePTime += System.currentTimeMillis() - makeTimeStart;    // todo
            long readLockTimeStart = System.currentTimeMillis();    // todo
            synchronized (reader) {
                long readTimeStart = System.currentTimeMillis();   // todo
                ts = reader.readTsNewByte(offset);

                readTime += (System.currentTimeMillis() - readTimeStart);   // todo
            }
            readLockTime += System.currentTimeMillis() - readLockTimeStart; // todo

            long disTimeStart = System.currentTimeMillis();    // todo
            if (Parameters.hasTimeStamp == 1) { // 有时间戳才需要判断原始时间序列的时间范围
                long timestamps = TsUtil.bytesToLong(ts, Parameters.tsDataSize);
                if (timestamps >= aQuery.startTime && timestamps <= aQuery.endTime) {
                    RawTs rawTs = new RawTs(ts, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts),  SearchUtil.longToBytes(aQuery.pArray[i]));
                    nearlyTsList.add(rawTs);
                }
            } else {
                RawTs rawTs = new RawTs(ts, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts),  SearchUtil.longToBytes(aQuery.pArray[i]));
                nearlyTsList.add(rawTs);
            }
            disTime += System.currentTimeMillis() - disTimeStart; // todo
        }
        forTime = System.currentTimeMillis() - forTimeStart;
        long sortTimeStart = System.currentTimeMillis();   // todo
        nearlyTsList.sort(new Comparator<RawTs>() {
            @Override
            public int compare(RawTs o1, RawTs o2) {
                return o1.dis.compareTo(o2.dis);
            }
        });
        sortTime = System.currentTimeMillis() - sortTimeStart;
        int cnt = 0;
        if (isExact) {
            // 返回若干个ares_exact(不含p)
            // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
            // ares_exact(没时间戳): ts 256*4, float dist 4
            byte[] tmp = new byte[aQuery.pArray.length * Parameters.exactResSize];
            for (RawTs rawTs : nearlyTsList) {
                float dis = rawTs.dis;
                if (cnt < aQuery.k && (dis < aQuery.topDist || cnt < aQuery.needNum)) { // 距离<topDist都要传给db用于剪枝
                    System.arraycopy(rawTs.ts, 0, tmp, cnt * Parameters.exactResSize, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.exactResSize + Parameters.tsSize, 4);
                    cnt++;
                }
                else {
                    break;
                }
            }
            byte[] aresExact = new byte[cnt * Parameters.exactResSize]; // aresExact: cnt个ares
            System.arraycopy(tmp, 0, aresExact, 0, cnt * Parameters.exactResSize);

            synchronized (SearchAction.class) { // todo
                Main.cntP += aQuery.pArray.length;
                Main.readTime += readTime;
                Main.totalReadLockTime += readLockTime;
                Main.cntRes += cnt;
            }
            PrintUtil.print("makeP时间：" + makePTime + " 读取时间：" + readTime + " readLockTime：" + readLockTime +
                    " 计算距离时间：" + disTime + " for总时间：" + forTime + " 排序总时间：" + sortTime +
                    " 查询个数：" + aQuery.pArray.length +  " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchRawTsTimeStart) +
                    " 线程：" + Thread.currentThread().getName() + "\n"); // todo

            return aresExact;
        }
        else {
            // 返回至多k个ares(含p)
            // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
            byte[] tmp = new byte[aQuery.pArray.length * Parameters.approximateResSize];
            for (RawTs rawTs : nearlyTsList) {
                float dis = rawTs.dis;
                if (cnt < aQuery.k && (dis < aQuery.topDist || cnt < aQuery.needNum)) {
                    System.arraycopy(rawTs.ts, 0, tmp, cnt * Parameters.approximateResSize, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.approximateResSize + Parameters.tsSize, 4);
                    System.arraycopy(rawTs.p, 0, tmp, cnt * Parameters.approximateResSize + Parameters.tsSize + 8, Parameters.pointerSize);

                    cnt ++ ;
                }
                else {
                    break;
                }
            }

            byte[] ares = new byte[cnt * Parameters.approximateResSize]; // ares: cnt个ares
            System.arraycopy(tmp, 0, ares, 0, cnt * Parameters.approximateResSize);
            tmp = null;
            synchronized (SearchAction.class) { // todo
                Main.cntP += aQuery.pArray.length;
                Main.readTime += readTime;
                Main.totalReadLockTime += readLockTime;
                Main.cntRes += cnt;
            }
            PrintUtil.print("makeP时间：" + makePTime + " 读取时间：" + readTime + " readLockTime：" + readLockTime +
                    " 计算距离时间：" + disTime + " for总时间：" + forTime + " 排序总时间：" + sortTime +
                    " 查询个数：" + aQuery.pArray.length +  " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchRawTsTimeStart) +
                    " 线程：" + Thread.currentThread().getName() + "\n"); // todo
            return ares;
        }
    }

    public static long[] searchRtree(RTree<String, Rectangle> rTree, long startTime, long endTime, byte[] minSaxT, byte[] maxSaxT) {
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
        PrintUtil.print("r树结果: sstableNum：" + Arrays.toString(sstableNum));

        byte[] ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);
        PrintUtil.print("近似查询结果长度" + ares.length);
        return ares;
    }

    // 近似查询,除了ares还返回sstableNum
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
        PrintUtil.print("近似r树结果: sstableNum：" + Arrays.toString(sstableNum));

        byte[] ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);

        PrintUtil.print("近似查询结果长度" + ares.length);
        return new Pair<>(ares, sstableNum);
    }




    public static byte[] searchTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        PrintUtil.print("近似查询===");
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaNum];
        PrintUtil.print("searchBytes " + Arrays.toString(searchTsBytes));
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxTData, paa);
        PrintUtil.print("saxT: "  + Arrays.toString(saxTData));
        PrintUtil.print("saxT的浮点值: " + VersionUtil.saxT2Double(saxTData));
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
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1,增大查询范围
        byte[] ares = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                rTree, amVersionID, stVersionID);

        while((ares.length - 4) / Parameters.approximateResSize < k && d > 0) { // 查询结果不够k个
            d --;
            ares = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                rTree, amVersionID, stVersionID);
        }

        // 释放版本
        VersionAction.unRefVersion(version);

        return ares;
    }

    public static byte[] searchExactTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        PrintUtil.print("精确查询===");
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaNum];
        PrintUtil.print("searchBytes " + Arrays.toString(searchTsBytes));
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxTData, paa);
        PrintUtil.print("saxT: "  + Arrays.toString(saxTData));
        PrintUtil.print("saxT的浮点值: " + VersionUtil.saxT2Double(saxTData));
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
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1

        Pair<byte[], long[]> aresAndSSNum = getAresAndSSTableFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                                                    rTree, amVersionID, stVersionID);
        byte[] approRes = aresAndSSNum.getKey();
        long[] approSSTableNum = aresAndSSNum.getValue();

        while((approRes.length - 4) / Parameters.approximateResSize < k && d > 0) { // 查询结果不够k个
            d --;
            approRes = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                        rTree, amVersionID, stVersionID);
        }

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

        byte[] exactRes;
        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String value = result.value();
            sstableNumList.add(Long.valueOf(value.substring(2 * Parameters.saxTSize)));
        }
        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();


        PrintUtil.print("精确查询版本: amVersionID:" + amVersionID + "\tstVersionID:" + stVersionID);
        PrintUtil.print("精确查询 r树结果:" + Arrays.toString(sstableNum) + "\t近似查询 r树结果:" + Arrays.toString(approSSTableNum));
        PrintUtil.print("aQuery长度 " + aQuery.length + "\t近似查询长度 " + approRes.length);

        exactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID, sstableNum, approRes, approSSTableNum);
        PrintUtil.print("精确查询结果长度" + exactRes.length);

        // 释放版本
        VersionAction.unRefVersion(version);

        return exactRes;
    }


}
