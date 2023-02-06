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
    public static synchronized byte[] searchOriTs(byte[] info, boolean isExact) { // todo
//    public static byte[] searchOriTs(byte[] info, boolean isExact) {
        long t6 = System.currentTimeMillis(); // todo
        System.out.println("查询原始时间序列 info长度" + info.length + " " + Thread.currentThread().getName() + " isExact " + isExact);
//        System.out.println();

        long readTime = 0;   // todo
        long readLockTime = 0;   // todo
        long makePTime = 0;   // todo
        long forTime = 0;
        long disTime = 0;
        long anaTime = 0;   // todo
        long t1 = System.currentTimeMillis(); // todo
        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        if (Parameters.hasTimeStamp > 0) {
            SearchUtil.analysisSearchSend(info, aQuery);
        } else {
            SearchUtil.analysisSearchSendHasNotTime(info, aQuery);
        }
        aQuery.sortPList();

//        System.out.println("解析完成");
//        System.out.println("解析时间：" + (System.currentTimeMillis() - t1));// todo


        long t5 = System.currentTimeMillis();   // todo
        List<OriTs> nearlyTsList = new ArrayList<>();
        for (int i = 0; i < aQuery.pList.size(); i ++ ) {
//            System.out.println("进入for");
            long makeTimeStart = System.currentTimeMillis();    // todo
            Long p = aQuery.pList.get(i);
//            System.out.println("p:" + p);
            int p_hash = (int) (p >> 56);   // 文件名
            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
//            System.out.println("p:" + p + " p_hash:" + p_hash + " offset:" + offset);
//            FileChannelReader reader = CacheUtil.fileChannelReaderMap.get(p_hash);
            MappedFileReader reader = CacheUtil.mappedFileReaderMap.get(p_hash);
            byte[] ts;
            makePTime += System.currentTimeMillis() - makeTimeStart;    // todo
            long readLockTimeStart = System.currentTimeMillis();    //
//            System.out.println("开始read");
            synchronized (reader) {
                long t2 = System.currentTimeMillis();   // todo
                if (i < Parameters.findOriTsNum) {  // new byte时间消耗很大，优先使用预先开好的空间，不足再新创
                    ts = reader.readTs(offset, i);
                }
                else {
                    ts = reader.readTsNewByte(offset);
                }

                readTime += (System.currentTimeMillis() - t2);   // todo
//                System.out.print(offset + " ");
            }
            readLockTime += System.currentTimeMillis() - readLockTimeStart; // todo

            long disTimeStart = System.currentTimeMillis();    // todo
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
            disTime += System.currentTimeMillis() - disTimeStart; // todo
        }
        forTime = System.currentTimeMillis() - t5;
//        System.out.println("for循环完成");
        long t3 = System.currentTimeMillis();   // todo
        nearlyTsList.sort(new Comparator<OriTs>() {
            @Override
            public int compare(OriTs o1, OriTs o2) {
                return o1.dis.compareTo(o2.dis);
            }
        });
//        System.out.println("排序时间：" + (System.currentTimeMillis() - t3));// todo
//        System.out.println();

        int cnt = 0;
        if (isExact) {
//            long t4 = System.currentTimeMillis();// todo
            // 返回若干个ares_exact(不含p)
            // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
            // ares_exact(没时间戳): ts 256*4, float dist 4
            byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresExactSize];
            for (OriTs oriTs : nearlyTsList) {
                float dis = oriTs.dis;
//                System.out.println("距离:" + dis);
                if (dis < aQuery.topDist || cnt < aQuery.needNum) { // 距离<topDist都要传给db用于剪枝
                    System.arraycopy(oriTs.ts, 0, tmp, cnt * Parameters.aresExactSize, Parameters.tsSize);
                    System.arraycopy(SearchUtil.floatToBytes(dis), 0, tmp, cnt * Parameters.aresExactSize + Parameters.tsSize, 4);
                    cnt++;
                }
                else {
                    break;
                }
            }
//            System.out.println("精确查询:" + " topDis:" + aQuery.topDist + " 需要个数:" + aQuery.needNum + " 访问ts个数:" + aQuery.pList.size() + " " + "返回ts个数:" + cnt);
            byte[] aresExact = new byte[cnt * Parameters.aresExactSize];  // aresExact: cnt个ares
            System.arraycopy(tmp, 0, aresExact, 0, cnt * Parameters.aresExactSize);
//            System.out.println("计算距离 生成ares时间：" + (System.currentTimeMillis() - t4));// todo

            synchronized (SearchAction.class) { // todo
                Main.cntP += aQuery.pList.size();
                Main.totalReadTime += readTime;
                Main.totalReadLockTime += readLockTime;
                Main.totalCntRes += cnt;
            }
            System.out.println("makeP时间：" + makePTime + " 读取时间：" + readTime + " readLockTime：" + readLockTime +
                    " 计算距离时间：" + disTime + " for总时间：" + forTime + " 排序总时间：" + (System.currentTimeMillis() - t3)
                    +" 查询个数：" + aQuery.pList.size() +  " 查询原始时间序列总时间：" + (System.currentTimeMillis() - t6) + " 线程：" + Thread.currentThread().getName() + "\n");// todo
            return aresExact;
        }
        else {
            // 返回至多k个ares(含p)
            // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
            // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
            byte[] tmp = new byte[aQuery.pList.size() * Parameters.aresSize];
            for (OriTs oriTs : nearlyTsList) {
                float dis = oriTs.dis;
//                System.out.println("距离:" + dis);
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
//            System.out.println("近似查询:" + " topDis:" + aQuery.topDist + " 需要个数:" + aQuery.needNum + " 访问ts个数:" + aQuery.pList.size() + " " + "返回ts个数:" + cnt);

            byte[] ares = new byte[cnt * Parameters.aresSize];  // aresExact: cnt个ares
            System.arraycopy(tmp, 0, ares, 0, cnt * Parameters.aresSize);
            tmp = null;
            synchronized (SearchAction.class) { // todo
                Main.cntP += aQuery.pList.size();
                Main.totalReadTime += readTime;
                Main.totalReadLockTime += readLockTime;
                Main.totalCntRes += cnt;
            }
            System.out.println("makeP时间：" + makePTime + " 读取时间：" + readTime + " readLockTime：" + readLockTime +
                    " 计算距离时间：" + disTime + " for总时间：" + forTime + " 排序总时间：" + (System.currentTimeMillis() - t3)
                    +" 查询个数：" + aQuery.pList.size() +  " 查询原始时间序列总时间：" + (System.currentTimeMillis() - t6) + " 线程：" + Thread.currentThread().getName() + "\n");// todo
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
//            String[] str = result.value().split(":");
//            if (SaxTUtil.compareSaxT(str[0].getBytes(StandardCharsets.ISO_8859_1), maxSaxT) <= 0 && SaxTUtil.compareSaxT(str[1].getBytes(StandardCharsets.ISO_8859_1), minSaxT) >= 0) {
//                sstableNumList.add(Long.valueOf(str[2]));
//            }

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
        System.out.println("r树结果: sstableNum：" + Arrays.toString(sstableNum));

        byte[] ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);
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
        System.out.println("近似r树结果: sstableNum：" + Arrays.toString(sstableNum));

        byte[] ares = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID, sstableNum);

        System.out.println("近似查询结果长度" + ares.length);
        return new Pair<>(ares, sstableNum);
    }


    public static byte[] searchTs(byte[] searchTsBytes, long startTime, long endTime, int k) {
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaNum];
        System.out.println("searchBytes " + Arrays.toString(searchTsBytes));
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
        VersionAction.unRefVersion(version);

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

        Iterable<Entry<String, Rectangle>> results;
        if (Parameters.hasTimeStamp > 0) {
            results = rTree.search(  // 所有saxT范围
                    Geometries.rectangle(-Double.MAX_VALUE, (double) startTime, Double.MAX_VALUE, (double) endTime)
            ).toBlocking().toIterable();
        }
        else {
            results = rTree.search(  // 所有saxT范围
                    Geometries.rectangle(-Double.MAX_VALUE, 0, Double.MAX_VALUE, 0)
            ).toBlocking().toIterable();
        }

        byte[] exactRes;
        ArrayList<Long> sstableNumList = new ArrayList<>();
        for (Entry<String, Rectangle> result : results) {
            String value = result.value();
            byte[] minSaxT = value.substring(0, Parameters.saxTSize).getBytes(StandardCharsets.ISO_8859_1);
            byte[] maxSaxT = value.substring(Parameters.saxTSize, 2 * Parameters.saxTSize).getBytes(StandardCharsets.ISO_8859_1);
            if (SaxTUtil.compareSaxT(minSaxT, saxTData) <= 0 && SaxTUtil.compareSaxT(maxSaxT, saxTData) >= 0) {
                sstableNumList.add(Long.valueOf(value.substring(2 * Parameters.saxTSize)));
            }
        }
        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();


        System.out.println("精确r树结果: sstableNum：" + Arrays.toString(sstableNum));

        System.out.println(amVersionID  + " " + stVersionID + " " + Arrays.toString(sstableNum) + " " + Arrays.toString(approSSTableNum));
        System.out.println("aQuery长度 " + aQuery.length + " " + "近似查询长度 " + approRes.length);

        exactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID, sstableNum, approRes, approSSTableNum);
        System.out.println("精确查询结果长度" + exactRes.length);

        // 释放版本
        VersionAction.unRefVersion(version);

        return exactRes;
    }


}
