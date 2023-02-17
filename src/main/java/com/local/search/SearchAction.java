package com.local.search;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.Main;
import com.local.domain.Parameters;
import com.local.domain.Version;
import com.local.util.*;
import com.local.version.VersionAction;
import com.local.version.VersionUtil;
import com.local.domain.Pair;

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

//    public static byte[] searchOriTs(byte[] info, boolean isExact) {
//        long searchOriTsTimeStart = System.currentTimeMillis(); // todo
//        PrintUtil.print("查询原始时间序列 info长度" + info.length + " " + Thread.currentThread().getName() + " isExact " + isExact);  // todo
//        long readTime = 0;   // todo
//        long readLockTime = 0;   // todo
//        long makePTime = 0;   // todo
//        long forTime;
//        long sortTime;
//        long disTime = 0;
//
//        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();
//
//        if (Parameters.hasTimeStamp > 0) {
//            SearchUtil.analysisSearchSend(info, aQuery);
//        } else {
//            SearchUtil.analysisSearchSendNoTime(info, aQuery);
//        }
//        aQuery.sortPList();
//
//        long forTimeStart = System.currentTimeMillis();   // todo
//        List<OriTs> nearlyTsList = new ArrayList<>();
//        for (int i = 0; i < aQuery.pList.size(); i ++ ) {
//            long makeTimeStart = System.currentTimeMillis();    // todo
//            Long p = aQuery.pList.get(i);
//
////            if (isExact) {
////                Long a = bitmap.get(p);
////                if (a != null && a == 1) {
////                    System.out.println("精确有近似 " + p);
////                }
////            }
//
//            int p_hash = (int) (p >> 56);   // 文件名
//            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
//            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
//            byte[] ts;
//            makePTime += System.currentTimeMillis() - makeTimeStart;    // todo
//            long readLockTimeStart = System.currentTimeMillis();    // todo
//            synchronized (reader) {
//                long readTimeStart = System.currentTimeMillis();   // todo
//                if (Parameters.isSearchMultithread) {
//                    ts = reader.readTsNewByte(offset);
//                }
//                else {
//                    if (i < Parameters.findOriTsNum) {  // new byte时间消耗很大，优先使用预先开好的空间，不足再新创
//                        ts = reader.readTs(offset, i);
//                    }
//                    else {
//                        ts = reader.readTsNewByte(offset);
//                    }
//                }
//                readTime += (System.currentTimeMillis() - readTimeStart);   // todo
//
//            }
//            readLockTime += System.currentTimeMillis() - readLockTimeStart; // todo
//
//            long disTimeStart = System.currentTimeMillis();    // todo
//            if (Parameters.hasTimeStamp == 1) { // 有时间戳才需要判断原始时间序列的时间范围
//                long timestamps = TsUtil.bytesToLong(ts, Parameters.timeSeriesDataSize);
//                if (timestamps >= aQuery.startTime && timestamps <= aQuery.endTime) {
//                    OriTs oriTs = new OriTs(ts, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts),  SearchUtil.longToBytes(aQuery.pList.get(i)));
//                    nearlyTsList.add(oriTs);
//                }
//            } else {
//                OriTs oriTs = new OriTs(ts, DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts),  SearchUtil.longToBytes(aQuery.pList.get(i)));
//                nearlyTsList.add(oriTs);
//            }
//            disTime += System.currentTimeMillis() - disTimeStart; // todo
//        }
//        forTime = System.currentTimeMillis() - forTimeStart;
//        long sortTimeStart = System.currentTimeMillis();   // todo
//        nearlyTsList.sort(new Comparator<OriTs>() {
//            @Override
//            public int compare(OriTs o1, OriTs o2) {
//                return o1.dis.compareTo(o2.dis);
//            }
//        });
//        sortTime = System.currentTimeMillis() - sortTimeStart;
//        int cnt = 0;
//        if (isExact) {
//            // 返回若干个ares_exact(不含p)
//            // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
//            // ares_exact(没时间戳): ts 256*4, float dist 4
//            byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresExactSize];
//            for (OriTs oriTs : nearlyTsList) {
//                float dis = oriTs.dis;
//                if (cnt < aQuery.k && (dis < aQuery.topDist || cnt < aQuery.needNum)) { // 距离<topDist都要传给db用于剪枝
//                    System.arraycopy(oriTs.ts, 0, tmp, cnt * Parameters.aresExactSize, Parameters.tsSize);
//                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.aresExactSize + Parameters.tsSize, 4);
//                    cnt++;
//                }
//                else {
//                    break;
//                }
//            }
//            byte[] aresExact = new byte[cnt * Parameters.aresExactSize]; // aresExact: cnt个ares
//            System.arraycopy(tmp, 0, aresExact, 0, cnt * Parameters.aresExactSize);
//
//            synchronized (SearchAction.class) { // todo
//                Main.cntP += aQuery.pList.size();
//                Main.totalReadTime += readTime;
//                Main.totalReadLockTime += readLockTime;
//                Main.totalCntRes += cnt;
//            }
//            PrintUtil.print("makeP时间：" + makePTime + " 读取时间：" + readTime + " readLockTime：" + readLockTime +
//                    " 计算距离时间：" + disTime + " for总时间：" + forTime + " 排序总时间：" + sortTime +
//                    " 查询个数：" + aQuery.pList.size() +  " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchOriTsTimeStart) +
//                    " 线程：" + Thread.currentThread().getName() + "\n"); // todo
//
//            return aresExact;
//        }
//        else {
//            // 返回至多k个ares(含p)
//            // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
//            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
//            byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresSize];
//            for (OriTs oriTs : nearlyTsList) {
//                float dis = oriTs.dis;
//                if (cnt < aQuery.k && (dis < aQuery.topDist || cnt < aQuery.needNum)) {
//                    System.arraycopy(oriTs.ts, 0, tmp, cnt * Parameters.aresSize, Parameters.tsSize);
//                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.aresSize + Parameters.tsSize, 4);
//                    System.arraycopy(oriTs.p, 0, tmp, cnt * Parameters.aresSize + Parameters.tsSize + 8, Parameters.pointerSize);
//
//                    cnt ++ ;
//                }
//                else {
//                    break;
//                }
//            }
//
//            byte[] ares = new byte[cnt * Parameters.aresSize]; // ares: cnt个ares
//            System.arraycopy(tmp, 0, ares, 0, cnt * Parameters.aresSize);
//            tmp = null;
//            synchronized (SearchAction.class) { // todo
//                Main.cntP += aQuery.pList.size();
//                Main.totalReadTime += readTime;
//                Main.totalReadLockTime += readLockTime;
//                Main.totalCntRes += cnt;
//            }
//            PrintUtil.print("makeP时间：" + makePTime + " 读取时间：" + readTime + " readLockTime：" + readLockTime +
//                    " 计算距离时间：" + disTime + " for总时间：" + forTime + " 排序总时间：" + sortTime +
//                    " 查询个数：" + aQuery.pList.size() +  " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchOriTsTimeStart) +
//                    " 线程：" + Thread.currentThread().getName() + "\n"); // todo
//            return ares;
//        }
//    }

    public static void searchOriTsPushHeap(byte[] info, boolean isExact) {
        long searchOriTsTimeStart = System.currentTimeMillis(); // todo
        PrintUtil.print("查询原始时间序列 info长度" + info.length + " " + Thread.currentThread().getName() + " isExact " + isExact);  // todo
        long readTime = 0;   // todo
        long readLockTime = 0;   // todo

        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        if (Parameters.hasTimeStamp > 0) {
            SearchUtil.analysisSearchSendHeap(info, aQuery);
        } else {
            SearchUtil.analysisSearchSendNoTimeHeap(info, aQuery);
        }
//        aQuery.sortPList();
        byte[] ares;
        if (isExact) {
            ares= new byte[Parameters.aresExactSize];
        }
        else {
            ares = new byte[Parameters.aresSize];
        }
        int cnt = 0;
        for (int i = 0; i < aQuery.pList.size(); i ++ ) {
            Long p = aQuery.pList.get(i);
            int p_hash = (int) (p >> 56);   // 文件名
            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
            byte[] ts;
            long readLockTimeStart = System.currentTimeMillis();    // todo
            float dis;
            synchronized (reader) {
                long readTimeStart = System.currentTimeMillis();   // todo
                ts = reader.readTs(offset);
                readTime += (System.currentTimeMillis() - readTimeStart);   // todo

                if (Parameters.hasTimeStamp == 1) { // 有时间戳才需要判断原始时间序列的时间范围
                    long timestamps = TsUtil.bytesToLong(ts, Parameters.timeSeriesDataSize);
                    if (timestamps < aQuery.startTime || timestamps > aQuery.endTime) {
                        continue;
                    }
                }
//                System.out.println(p + " " + DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts));   // todo todo
                dis = DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts);
                if (isExact) {
                    // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
                    // ares_exact(没时间戳): ts 256*4, float dist 4
                    System.arraycopy(ts, 0, ares, 0, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, ares, Parameters.tsSize, 4);
                } else {
                    // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
                    // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
                    System.arraycopy(ts, 0, ares, 0, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, ares, Parameters.tsSize, 4);
                    System.arraycopy(SearchUtil.longToBytes(aQuery.pList.get(i)), 0, ares, Parameters.tsSize + 8, Parameters.pointerSize);
                }
            }
            readLockTime += System.currentTimeMillis() - readLockTimeStart; // todo

            if (aQuery.needNum > 0) {
                cnt ++ ;
                aQuery.topDist = DBUtil.dataBase.heap_push(ares, aQuery.heap);
                aQuery.needNum -- ;
            }
           else {
               if (dis < aQuery.topDist) {
                   cnt ++ ;
                   aQuery.topDist = DBUtil.dataBase.heap_push(ares, aQuery.heap);
               }
           }
        }

        synchronized (SearchAction.class) { // todo
            Main.cntP += aQuery.pList.size();
            Main.totalReadTime += readTime;
            Main.totalReadLockTime += readLockTime;
            Main.totalCntRes += cnt;
        }

        PrintUtil.print(" 读取时间：" + readTime + " readLockTime：" + readLockTime +
                " 查询个数：" + aQuery.pList.size() + " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchOriTsTimeStart) +
                " 线程：" + Thread.currentThread().getName() + "\n");  // todo
    }


//    public static void searchOriTsPushHeapQueue(byte[] info, boolean isExact) {
//        long searchOriTsTimeStart = System.currentTimeMillis(); // todo
//        PrintUtil.print("查询原始时间序列 info长度" + info.length + " " + Thread.currentThread().getName() + " isExact " + isExact);  // todo
//        long readTime = 0;   // todo
//        long readLockTime = 0;   // todo
//
//        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();
//
//        if (Parameters.hasTimeStamp > 0) {
//            SearchUtil.analysisSearchSendHeap(info, aQuery);
//        } else {
//            SearchUtil.analysisSearchSendNoTimeHeap(info, aQuery);
//        }
//        aQuery.sortPList();
//        byte[] ares;
//        if (isExact) {
//            ares = new byte[Parameters.aresExactSize];
//        }
//        else {
//            ares = new byte[Parameters.aresSize];
//        }
//
//        for (int i = 0; i < aQuery.pList.size(); i ++ ) {
//            Long p = aQuery.pList.get(i);
//            int p_hash = (int) (p >> 56);   // 文件名
//            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
//            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
//            byte[] ts;
//            long readLockTimeStart = System.currentTimeMillis();    // todo
//            float dis;
//            synchronized (reader) {
//                long readTimeStart = System.currentTimeMillis();   // todo
//                ts = reader.readTs(offset, 0);
//                readTime += (System.currentTimeMillis() - readTimeStart);   // todo
//
//                if (Parameters.hasTimeStamp == 1) { // 有时间戳才需要判断原始时间序列的时间范围
//                    long timestamps = TsUtil.bytesToLong(ts, Parameters.timeSeriesDataSize);
//                    if (timestamps < aQuery.startTime || timestamps > aQuery.endTime) {
//                        continue;
//                    }
//                }
//
//                dis = DBUtil.dataBase.dist_ts(aQuery.timeSeriesData, ts);
//                if (isExact) {
//                    // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
//                    // ares_exact(没时间戳): ts 256*4, float dist 4
//                    System.arraycopy(ts, 0, ares, 0, Parameters.tsSize);
//                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, ares, Parameters.tsSize, 4);
//                } else {
//                    // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
//                    // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
//                    System.arraycopy(ts, 0, ares, 0, Parameters.tsSize);
//                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, ares, Parameters.tsSize, 4);
//                    System.arraycopy(aQuery.pBytesList.get(i), 0, ares, Parameters.tsSize + 8, 4);
//                }
//            }
//            readLockTime += System.currentTimeMillis() - readLockTimeStart; // todo
//            if (aQuery.needNum > 0) {
//                aQuery.topDist = DBUtil.dataBase.heap_push(ares, aQuery.heap);
//                aQuery.needNum -- ;
//            }
//            else {
//                if (dis < aQuery.topDist) {
//                    aQuery.topDist = DBUtil.dataBase.heap_push(ares, aQuery.heap);
//                }
//            }
//        }
//
//        PrintUtil.print(" 读取时间：" + readTime + " readLockTime：" + readLockTime +
//                " 查询个数：" + aQuery.pList.size() + " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchOriTsTimeStart) +
//                " 线程：" + Thread.currentThread().getName() + "\n");  // todo
//    }

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

        while((ares.length - 4) / Parameters.aresSize < k && d > 0) { // 查询结果不够k个
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

        while((approRes.length - 4) / Parameters.aresSize < k && d > 0) { // 查询结果不够k个
            d --;
            approRes = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                                        rTree, amVersionID, stVersionID);
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

        byte[] exactRes;
        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String value = result.value();
            sstableNumList.add(Long.valueOf(value.substring(2 * Parameters.saxTSize)));
        }
        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();


        PrintUtil.print("精确查询版本: amVersionID:" + amVersionID + "\tstVersionID:" + stVersionID);
        PrintUtil.print("精确查询 r树结果:" + Arrays.toString(sstableNum) + "\t近似查询 r树结果:" + Arrays.toString(approSSTableNum));
        System.out.println("精确查询 r树结果:" + Arrays.toString(sstableNum) + "\t近似查询 r树结果:" + Arrays.toString(approSSTableNum)); // todo todo
        PrintUtil.print("aQuery长度 " + aQuery.length + "\t近似查询长度 " + approRes.length);

        exactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID, sstableNum, approRes, approSSTableNum);
        PrintUtil.print("精确查询结果长度" + exactRes.length);

        // 释放版本
        VersionAction.unRefVersion(version);

        return exactRes;
    }


}
