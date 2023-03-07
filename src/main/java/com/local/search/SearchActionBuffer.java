package com.local.search;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.Main;
import com.local.domain.Pair;
import com.local.domain.Parameters;
import com.local.domain.Version;
import com.local.util.*;
import com.local.version.VersionAction;
import com.local.version.VersionUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SearchActionBuffer {
    static byte[] empty4Bytes = new byte[4];
    public static int pushHeap(List<ByteBuffer> byteBufferList, ByteBuffer res, List<Long> pList, SearchUtil.SearchContent aQuery, boolean isExact) {
        float dis;
        int cnt = 0;
        for (int i = 0; i < pList.size(); i ++ ) {
            ByteBuffer tsBuffer = byteBufferList.get(i);
            if (Parameters.hasTimeStamp == 1) { // 有时间戳才需要判断原始时间序列的时间范围
                tsBuffer.position(Parameters.tsDataSize);
                long timestamps = tsBuffer.getLong();

                if (timestamps < aQuery.startTime || timestamps > aQuery.endTime) {
                    continue;
                }
            }
//            PrintUtil.printTsBuffer(tsBuffer);
            dis = DBUtil.dataBase.dist_ts_buffer(aQuery.tsBuffer, tsBuffer);
//            System.out.println("距离" + dis);
            tsBuffer.rewind();
            res.clear();
            if (isExact) {
                // ares_exact(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐)
                // ares_exact(没时间戳): ts 256*4, float dist 4
                res.put(tsBuffer);
                res.putFloat(dis);
            } else {
                // ares(有时间戳): ts 256*4, long time 8, float dist 4, 空4位(time是long,对齐), long p 8
                // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), long p 8
                res.put(tsBuffer);
                res.putFloat(dis);
                res.put(empty4Bytes);
                res.putLong(pList.get(i));
            }
            if (aQuery.needNum > 0) {
                cnt ++ ;
                if (isExact) {
                    aQuery.topDist = DBUtil.dataBase.heap_push_exact_buffer(res, aQuery.heap);
                }
                else {
                    aQuery.topDist = DBUtil.dataBase.heap_push_buffer(res, aQuery.heap);
                }
                aQuery.needNum -- ;
            }
            else {
                if (dis < aQuery.topDist) {
                    cnt ++ ;
                    if (isExact) {
                        aQuery.topDist = DBUtil.dataBase.heap_push_exact_buffer(res, aQuery.heap);
                    }
                    else {
                        aQuery.topDist = DBUtil.dataBase.heap_push_buffer(res, aQuery.heap);
                    }
                }
            }
        }
        return cnt;
    }
    static ByteBuffer ares = ByteBuffer.allocateDirect(Parameters.approximateResSize).order(ByteOrder.LITTLE_ENDIAN);
    static ByteBuffer exactRes = ByteBuffer.allocateDirect(Parameters.exactResSize).order(ByteOrder.LITTLE_ENDIAN);
    public static void searchOriTsHeapQueue(ByteBuffer info, boolean isExact) {
        long searchOriTsTimeStart = System.currentTimeMillis(); // todo
        PrintUtil.print("查询原始时间序列 info长度" + info.capacity() + " " + Thread.currentThread().getName() + " isExact " + isExact);  // todo
        long readTime = 0;   // todo
        long readLockTime = 0;   // todo

        SearchUtil.SearchContent aQuery = new SearchUtil.SearchContent();

        if (Parameters.hasTimeStamp > 0) {
            SearchUtil.analysisInfoHeap(info, aQuery);
        } else {
            SearchUtil.analysisInfoNoTimeHeap(info, aQuery);
        }
        if (Parameters.findOriTsSort) {
            aQuery.sortPList();
        }

//        System.out.println("aQuery时间:" + aQuery.startTime + " " + aQuery.endTime);

        ByteBuffer res;
        if(isExact) res = exactRes;
        else res = ares;


        int cnt = 0;
        Set<MappedFileReaderBuffer> readerSet = new HashSet<>();
        for (int i = 0; i < aQuery.pList.size(); i ++ ) {
            Long p = aQuery.pList.get(i);
            int p_hash = (int) (p >> 56);   // 文件名

//            // todo todo
//            if (approbitmap.get(p) != null) {
//                System.out.println("精确有近似的p!!! " + p);
//            }

            MappedFileReaderBuffer reader = CacheUtil.mappedFileReaderMapBuffer.get(p_hash);
            readerSet.add(reader);

            long readLockTimeStart = System.currentTimeMillis();    // todo
            synchronized (reader) {
                List<Long> pList = reader.getPList();
                pList.add(p);
                if (pList.size() >= Parameters.FileSetting.queueSize) {
                    long readTimeStart = System.currentTimeMillis();   // todo
                    List<ByteBuffer> byteBufferList = reader.readTsQueue();
                    readTime += (System.currentTimeMillis() - readTimeStart);   // todo

                    cnt += pushHeap(byteBufferList, res, pList, aQuery, isExact);
//                    System.out.println("push heap 成功");
                    reader.clearPList();
                }
            }
            readLockTime += System.currentTimeMillis() - readLockTimeStart; // todo
        }
        for (MappedFileReaderBuffer reader : readerSet) { // 剩下不足Parameters.FileSetting.queueSize的
            long readTimeStart = System.currentTimeMillis();   // todo
            List<ByteBuffer> byteBufferList = reader.readTsQueue();
            readTime += (System.currentTimeMillis() - readTimeStart);   // todo
            List<Long> pList = reader.getPList();
            cnt += pushHeap(byteBufferList, res, pList, aQuery, isExact);
            reader.clearPList();
        }
        if (Main.isRecord) {
            synchronized (SearchAction.class) { // todo
                Main.cntP += aQuery.pList.size();
                Main.totalReadTime += readTime;
                Main.totalReadLockTime += readLockTime;
                Main.cntRes += cnt;
            }
        }

        PrintUtil.print(" 读取时间：" + readTime + " readLockTime：" + readLockTime +
                " 查询个数：" + aQuery.pList.size() + " 查询原始时间序列总时间：" + (System.currentTimeMillis() - searchOriTsTimeStart) +
                " 线程：" + Thread.currentThread().getName() + "\n");  // todo
    }



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
//            System.out.println("minsax" + Arrays.toString(minSaxT));
            byte[] maxSaxT = SaxTUtil.makeMaxSaxT(saxTData, d);
//            System.out.println("maxsax" + Arrays.toString(maxSaxT));
            sstableNumBuffer = searchRtree(rTree, startTime, endTime, minSaxT, maxSaxT);
        }
//        PrintUtil.print("r树结果: sstableNum：" + Arrays.toString(sstableNum));
        PrintUtil.print("r树结果： 个数：" + sstableNumBuffer.capacity() / 8);
        PrintUtil.printSSTableBuffer(sstableNumBuffer);

        // Get返回若干个ares,ares的最后有一个4字节的id,用于标记近似查的是当前am版本中的哪个表(一个am版本有多个表并行维护不同的saxt树),用于精准查询的appro_res(去重)
        ByteBuffer approRes = ByteBuffer.allocateDirect(k * Parameters.approximateResSize + 4).order(ByteOrder.LITTLE_ENDIAN);   // 空的ByteBuffer给C写
        ByteBuffer infoBuffer;
        if (Parameters.hasTimeStamp > 0) {
            infoBuffer = ByteBuffer.allocateDirect(Parameters.tsSize + 32 + Parameters.infoMaxPSize * 8).order(ByteOrder.LITTLE_ENDIAN); // 空的ByteBuffer给C写
        }
        else {
            infoBuffer = ByteBuffer.allocateDirect(Parameters.tsSize + 24 + Parameters.infoMaxPSize * 8).order(ByteOrder.LITTLE_ENDIAN); // 空的ByteBuffer给C写
        }
        int numAres = DBUtil.dataBase.Get(aQuery, isUseAm, amVersionID, stVersionID,
                sstableNumBuffer.capacity() / 8, sstableNumBuffer, approRes, infoBuffer);
//        PrintUtil.print("近似查询结果长度" + approRes.capacity());
        return new Pair<>(new Pair<>(numAres, approRes), sstableNumBuffer);
    }


    public static ByteBuffer searchTs(ByteBuffer searchTsBuffer, long startTime, long endTime, int k) {
        PrintUtil.print("近似查询===");
        boolean isUseAm = true; // saxT范围 是否在个该机器上
        ByteBuffer saxTBuffer = ByteBuffer.allocateDirect(Parameters.saxTSize);
        ByteBuffer paaBuffer = ByteBuffer.allocateDirect(Parameters.paaNum * 4);

        DBUtil.dataBase.paa_saxt_from_ts_buffer(searchTsBuffer, saxTBuffer, paaBuffer);
//        PrintUtil.printTsBuffer(searchTsBuffer);

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
        saxTBuffer.rewind();
        byte[] saxTData = new byte[saxTBuffer.remaining()];
        saxTBuffer.get(saxTData);
        PrintUtil.print("saxT:" + Arrays.toString(saxTData));
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1,增大查询范围
        Pair<Integer, ByteBuffer> aresPair = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                rTree, amVersionID, stVersionID, k).getKey();
        int numAres = aresPair.getKey();
        ByteBuffer ares = aresPair.getValue();
        while(numAres < k && d > 0) { // 查询结果不够k个
            d --;
            System.out.println("近似结果 < k 个, d:" + d + " 时间" + startTime + " " + endTime);
            aresPair = getAresFromDB(isUseAm, startTime, endTime, saxTData, aQuery, d,
                    rTree, amVersionID, stVersionID, k).getKey();
            ares = aresPair.getValue();
            numAres = aresPair.getKey();
            System.out.println("近似结果个数 " + numAres);
        }

//         释放版本
        VersionAction.unRefVersion(version);

        return ares;
    }


    public static ByteBuffer searchExactTs(ByteBuffer searchTsBuffer, long startTime, long endTime, int k) {
        PrintUtil.print("精确查询===");
//        System.out.println("时间 " + startTime + " " + endTime);
        boolean isUseAm = true; // saxT范围 是否在个该机器上
//        byte[] saxTData = new byte[Parameters.saxTSize];
        ByteBuffer saxTBuffer = ByteBuffer.allocateDirect(Parameters.saxTSize);
//        float[] paa = new float[Parameters.paaNum];
        ByteBuffer paaBuffer = ByteBuffer.allocateDirect(Parameters.paaNum * 4);
        PrintUtil.print("searchBuffer " + searchTsBuffer.toString());
//        PrintUtil.printTsBuffer(searchTsBuffer);
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
        saxTBuffer.rewind();
        byte[] saxTData = new byte[saxTBuffer.remaining()];
        saxTBuffer.get(saxTData);
        int d = Parameters.bitCardinality;  // 相聚度,开始为bitCardinality,找不到k个则-1
        Pair<Pair<Integer, ByteBuffer>, ByteBuffer> aresAndSSNum = getAresFromDB(isUseAm, startTime, endTime, saxTData,
                aQuery, d, rTree, amVersionID, stVersionID, k);
        Pair<Integer, ByteBuffer> aresPair = aresAndSSNum.getKey();
        ByteBuffer approSSTableNum = aresAndSSNum.getValue();
        int numAres = aresPair.getKey();
        ByteBuffer approRes = aresPair.getValue();
        while(numAres < k && d > 0) { // 查询结果不够k个
            d --;
            System.out.println("近似结果 < k 个, d:" + d + " 时间" + startTime + " " + endTime);
            aresAndSSNum = getAresFromDB(isUseAm, startTime, endTime, saxTData,
                    aQuery, d, rTree, amVersionID, stVersionID, k);
            aresPair = aresAndSSNum.getKey();
            approSSTableNum = aresAndSSNum.getValue();
            approRes = aresPair.getValue();
            numAres = aresPair.getKey();
            System.out.println("近似结果个数 " + numAres);
        }

//        checkDis(approRes, searchTsBuffer);

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


//        System.out.println("近似sstable:");
//        PrintUtil.printSSTableBuffer(approSSTableNum);

        ByteBuffer exactRes = ByteBuffer.allocateDirect(k * Parameters.exactResSize).order(ByteOrder.LITTLE_ENDIAN);   // 空的ByteBuffer给C写
        ByteBuffer infoBuffer;
        if (Parameters.hasTimeStamp > 0) {
            infoBuffer = ByteBuffer.allocateDirect(Parameters.tsSize + 32 + Parameters.infoMaxPSize * 8).order(ByteOrder.LITTLE_ENDIAN); // 空的ByteBuffer给C写
        }
        else {
            infoBuffer = ByteBuffer.allocateDirect(Parameters.tsSize + 24 + Parameters.infoMaxPSize * 8).order(ByteOrder.LITTLE_ENDIAN); // 空的ByteBuffer给C写
        }

        int numExactRes = DBUtil.dataBase.Get_exact(aQuery, amVersionID, stVersionID,
                sstableNumList.size(), sstableNumBuffer, numAres, approRes,
                approSSTableNum.capacity() / 8, approSSTableNum, exactRes, infoBuffer);
//        PrintUtil.print("精确查询结果长度" + exactRes.length);

        // 释放版本
        VersionAction.unRefVersion(version);

        return exactRes;
    }

    public static void checkDis(ByteBuffer approRes, ByteBuffer searchTsBuffer) {
        //        // todo
        System.out.println("判断近似结果正确");
        byte[] ares = new byte[approRes.capacity()];
        approRes.get(ares);
        approRes.rewind();
        searchTsBuffer.rewind();
        byte[] searchTsBytes = new byte[Parameters.tsSize];
        searchTsBuffer.get(searchTsBytes);
        searchTsBuffer.rewind();

        approbitmap.clear();

        for (int i = 0; i < ares.length - 4; i += Parameters.approximateResSize) {
            byte[] tsBytes = new byte[Parameters.tsDataSize];
            System.arraycopy(ares, i, tsBytes, 0, Parameters.tsDataSize);
            byte[] floatBytes = new byte[4];
            System.arraycopy(ares, i + Parameters.tsSize, floatBytes, 0, 4);
            byte[] pBytes = new byte[8];
            System.arraycopy(ares, i + Parameters.tsSize + 8, pBytes, 0, 8);
            long p = SearchUtil.bytesToLong(pBytes);
            if (approbitmap.get(p) != null) {
                System.out.println(" 近似p有重复！！！ " + p);
            }
            approbitmap.put(p, 1L);
            int p_hash = (int) (p >> 56);   // 文件名
            long offset = p & 0x00ffffffffffffffL;  // ts在文件中的位置
            MappedFileReaderBuffer reader = CacheUtil.mappedFileReaderMapBuffer.get(p_hash);
            byte[] ts;
            synchronized (reader) {
                ts = reader.readTsNewByte(offset);
            }
            for (int j = 0; j < Parameters.tsSize; j ++ ) {
                if (ts[j] != tsBytes[j]) {
                    System.out.println("ts错误！！！！！！！！！！！！！");

                    System.out.println(Arrays.toString(ts).substring(0,100));
                    System.out.println(Arrays.toString(tsBytes).substring(0,100));
                    break;
                }

                if (SearchUtil.bytesToFloat(floatBytes) != DBUtil.dataBase.dist_ts(ts, searchTsBytes)) {
                    System.out.println("距离错误！！！！！！！！！！！！！");

                    System.out.println(Arrays.toString(ts).substring(0,100));
                    System.out.println(Arrays.toString(tsBytes).substring(0,100));
                }
            }
        }
        System.out.println("判断完毕");
    }
    static Map<Long, Long> approbitmap = new HashMap<>();
}
