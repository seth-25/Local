package com.local.version;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.domain.Parameters;
import com.local.domain.Version;
import com.local.util.CacheUtil;
import com.local.util.DBUtil;
import javafx.util.Pair;

import java.util.*;

public class VersionAction {
    public static synchronized void unRefCurVersion() {
        CacheUtil.curVersion.unRef();
        if (CacheUtil.curVersion.getRef() == 0) {
            HashMap<String, Pair<Integer, Integer>> workerVersions = CacheUtil.curVersion.getWorkerVersions();
            unRefWorkerVersions(workerVersions);
        }
    }

    private static synchronized void unRefWorkerVersions(HashMap<String, Pair<Integer, Integer>> workerVersions) {
        for (Map.Entry<String, Pair<Integer, Integer>> entry: workerVersions.entrySet()) {
            String hostName = entry.getKey();
            if (entry.getValue() == null) continue;

            int inVer = entry.getValue().getKey();
            int outVer = entry.getValue().getValue();

            HashMap<Integer, Integer> workerInVersionMap = CacheUtil.workerInVerRef.get(hostName);
            workerInVersionMap.put(inVer, workerInVersionMap.get(inVer) - 1);

            workerInVersionMap = CacheUtil.workerOutVerRef.get(hostName);
            workerInVersionMap.put(outVer, workerInVersionMap.get(outVer) - 1);

        }
    }

    private static synchronized void refWorkerVersions(HashMap<String, Pair<Integer, Integer>> workerVersions) {
        for (Map.Entry<String, Pair<Integer, Integer>> entry: workerVersions.entrySet()) {
            String hostName = entry.getKey();
            if (entry.getValue() == null) continue;

            int inVer = entry.getValue().getKey();
            int outVer = entry.getValue().getValue();

            HashMap<Integer, Integer> workerInVersionMap = CacheUtil.workerInVerRef.get(hostName);
            if (workerInVersionMap.containsKey(inVer)) {
                workerInVersionMap.put(inVer, workerInVersionMap.get(inVer) + 1);
            }
            else {
                workerInVersionMap.put(inVer, 1);
            }

            HashMap<Integer, Integer> workerOutVersionMap = CacheUtil.workerOutVerRef.get(hostName);
            if (workerOutVersionMap.containsKey(outVer)) {
                workerOutVersionMap.put(outVer, workerOutVersionMap.get(outVer) + 1);
            }
            else {
                workerOutVersionMap.put(outVer, 1);
            }
        }
    }

    public static synchronized void refCurVersion() {
        CacheUtil.curVersion.addRef();
    }

    public static synchronized void updateCurVersion(Version version) {
        CacheUtil.curVersion = version;
    }


    public static synchronized void initVersion() {
        RTree<String, Rectangle> tree = RTree.create();
        HashMap<String, Pair<Integer, Integer>> workerVersions = new HashMap<>();
        workerVersions.put(Parameters.hostName, null);
        Version version = new Version(tree, workerVersions);
        version.setRef(1);

        CacheUtil.curVersion = version;
    }

    public static void changeVersion(byte[] versionBytes, String workerHostName) {
        Version newVersion = CacheUtil.curVersion.deepCopy();
//        System.out.println("---------------------------------");
        if (versionBytes[0] == 0) {
            VersionUtil.Version1Content ver1 = new VersionUtil.Version1Content();

            VersionUtil.analysisVersionBytes(versionBytes, ver1);

            System.out.println("\t需要更新版本1:" + "内" + ver1.inVer + " 外" + ver1.outVer + " 文件"
                    + ver1.fileNum + " " + ver1.minTime + " " + ver1.maxTime);
            newVersion.updateVersion(workerHostName, new Pair<>(ver1.inVer, ver1.outVer));


            // rtree插入
            RTree<String, Rectangle> tree = newVersion.getrTree();
            System.out.println("\trtree插入:" + VersionUtil.saxToDouble(ver1.minSax) + " " + VersionUtil.saxToDouble(ver1.maxSax) + " 文件名" + ver1.fileNum);
//            System.out.println("\trtree插入sax(long):" + VersionUtil.bytesToLong(minSax) + " " + VersionUtil.bytesToLong(maxSax));
//            System.out.println("\trtree插入sax(byte):" + Arrays.toString(minSax) + " " + Arrays.toString(maxSax));
            tree = tree.add(Parameters.hostName + ":" + ver1.fileNum, Geometries.rectangle (
                    VersionUtil.saxToDouble(ver1.minSax), (double) ver1.minTime,
                    VersionUtil.saxToDouble(ver1.maxSax), (double) ver1.maxTime));

            newVersion.setrTree(tree);
            System.out.println("\trtree插入完成");

            newVersion.setRef(1);    // 大版本ref=1
            refWorkerVersions(newVersion.getWorkerVersions());  // 该大版本包括所有worker的小版本ref+1

            System.out.println("\t当前版本ref" + CacheUtil.curVersion.getRef());
            unRefCurVersion();  // 上一个大版本ref-1
//            System.out.println("当前版本ref" + CacheUtil.curVersion.getRef());
            updateCurVersion(newVersion);   // 新版本覆盖旧版本

            System.out.println("\t更新完成 " + CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName));
            printWorkerVersion();
        }
        else if (versionBytes[0] == 1) {
            Integer[] outVer = {0};
            VersionUtil.Version2Content ver2 = new VersionUtil.Version2Content();
            VersionUtil.analysisVersionBytes(versionBytes, ver2);

            newVersion.updateVersion(workerHostName, outVer[0]);

            // rtree删除
            RTree<String, Rectangle> tree = newVersion.getrTree();

            for (int i = 0; i < ver2.delMaxSaxes.size(); i ++ ) {
                tree = tree.delete(Parameters.hostName + ":" + ver2.delFileNums.get(i), Geometries.rectangle (
                        VersionUtil.saxToDouble(ver2.delMinSaxes.get(i)), (double) ver2.delMinTimes.get(i),
                        VersionUtil.saxToDouble(ver2.delMaxSaxes.get(i)),(double) ver2.delMaxTimes.get(i)));
            }
            // rtree增加
            for (int i = 0; i < ver2.addMaxSaxes.size(); i ++ ) {
                tree = tree.add(Parameters.hostName + ":" + ver2.addFileNums.get(i), Geometries.rectangle (
                        VersionUtil.saxToDouble(ver2.addMinSaxes.get(i)), (double) ver2.addMinTimes.get(i),
                        VersionUtil.saxToDouble(ver2.addMaxSaxes.get(i)), (double) ver2.addMaxTimes.get(i)));
            }
            newVersion.setrTree(tree);

            newVersion.setRef(1);
            refWorkerVersions(newVersion.getWorkerVersions());
            unRefCurVersion();
            updateCurVersion(newVersion);

        } else {
            throw new RuntimeException("版本错误");
        }
    }

//    public static void changeVersion(byte[] versionBytes, String workerHostName) {
//        Version newVersion = CacheUtil.curVersion.deepCopy();
////        System.out.println("---------------------------------");
//        if (versionBytes[0] == 0) {
//            Integer[] inVer = {0};
//            Integer[] outVer = {0};
//            Long[] fileNum = {0L};
//            byte[] minSax = new byte[Parameters.saxDataSize];
//            byte[] maxSax = new byte[Parameters.saxDataSize];
//            Long[] minTime = {0L};
//            Long[] maxTime = {0L};
//
//            VersionUtil.analysisVersionBytes(versionBytes,
//                    inVer, outVer, fileNum, minSax, maxSax, minTime, maxTime);
//
//            System.out.println("\t需要更新版本1:" + "内" + inVer[0] + " 外" + outVer[0] + " 文件" + fileNum[0] + " " + minTime[0] + " " + maxTime[0]);
//            newVersion.updateVersion(workerHostName, new Pair<>(inVer[0], outVer[0]));
//
//
//            // rtree插入
//            RTree<String, Rectangle> tree = newVersion.getrTree();
//            System.out.println("\trtree插入:" + VersionUtil.saxToDouble(minSax) + " " + VersionUtil.saxToDouble(maxSax) + " 文件名" + fileNum[0]);
////            System.out.println("\trtree插入sax(long):" + VersionUtil.bytesToLong(minSax) + " " + VersionUtil.bytesToLong(maxSax));
////            System.out.println("\trtree插入sax(byte):" + Arrays.toString(minSax) + " " + Arrays.toString(maxSax));
//            tree = tree.add(Parameters.hostName + ":" + fileNum[0], Geometries.rectangle (
//                    VersionUtil.saxToDouble(minSax), (double) minTime[0],
//                    VersionUtil.saxToDouble(maxSax), (double) maxTime[0]));
//
//            newVersion.setrTree(tree);
//            System.out.println("\trtree插入完成");
//
//            newVersion.setRef(1);    // 大版本ref=1
//            refWorkerVersions(newVersion.getWorkerVersions());  // 该大版本包括所有worker的小版本ref+1
//
//            System.out.println("\t当前版本ref" + CacheUtil.curVersion.getRef());
//            unRefCurVersion();  // 上一个大版本ref-1
////            System.out.println("当前版本ref" + CacheUtil.curVersion.getRef());
//            updateCurVersion(newVersion);   // 新版本覆盖旧版本
//
//            System.out.println("\t更新完成 " + CacheUtil.curVersion.getWorkerVersions().get(Parameters.hostName));
//            printWorkerVersion();
//        }
//        else if (versionBytes[0] == 1) {
//            Integer[] outVer = {0};
//            ArrayList<Long> addFileNums = new ArrayList<>();
//            ArrayList<byte[]> addMinSaxes = new ArrayList<>();
//            ArrayList<byte[]> addMaxSaxes = new ArrayList<>();
//            ArrayList<Long> addMinTimes = new ArrayList<>();
//            ArrayList<Long> addMaxTimes = new ArrayList<>();
//            ArrayList<Long> delFileNums = new ArrayList<>();
//            ArrayList<byte[]> delMinSaxes = new ArrayList<>();
//            ArrayList<byte[]> delMaxSaxes = new ArrayList<>();
//            ArrayList<Long> delMinTimes = new ArrayList<>();
//            ArrayList<Long> delMaxTimes = new ArrayList<>();
//            VersionUtil.analysisVersionBytes(versionBytes, outVer,
//                    addFileNums, addMinSaxes, addMaxSaxes, addMinTimes, addMaxTimes,
//                    delFileNums, delMinSaxes, delMaxSaxes, delMinTimes, delMaxTimes);
//
//            newVersion.updateVersion(workerHostName, outVer[0]);
//
//            // rtree删除
//            RTree<String, Rectangle> tree = newVersion.getrTree();
//
//            for (int i = 0; i < delMaxSaxes.size(); i ++ ) {
//                tree = tree.delete(Parameters.hostName + ":" + delFileNums.get(i), Geometries.rectangle (
//                        VersionUtil.saxToDouble(delMinSaxes.get(i)), (double) delMinTimes.get(i),
//                        VersionUtil.saxToDouble(delMaxSaxes.get(i)),(double) delMaxTimes.get(i)));
//            }
//            // rtree增加
//            for (int i = 0; i < addMaxSaxes.size(); i ++ ) {
//                tree = tree.add(Parameters.hostName + ":" + addFileNums.get(i), Geometries.rectangle (
//                        VersionUtil.saxToDouble(addMinSaxes.get(i)), (double) addMinTimes.get(i),
//                        VersionUtil.saxToDouble(addMaxSaxes.get(i)), (double) addMaxTimes.get(i)));
//            }
//            newVersion.setrTree(tree);
//
//            newVersion.setRef(1);
//            refWorkerVersions(newVersion.getWorkerVersions());
//            unRefCurVersion();
//            updateCurVersion(newVersion);
//
//        } else {
//            throw new RuntimeException("版本错误");
//        }
//    }



    public static void checkWorkerVersion() {   // 检查worker的小版本的ref，如果=0则通知worker删除版本
        for (Map.Entry<String, HashMap<Integer, Integer>> oneWorkerInVer: CacheUtil.workerInVerRef.entrySet()) {
            String hostName = oneWorkerInVer.getKey();
            HashMap<Integer, Integer> workerInVersionMap = oneWorkerInVer.getValue();
            Iterator<Map.Entry<Integer, Integer>> iterator = workerInVersionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                int version = entry.getKey();
                int ref = entry.getValue();
                if (ref == 0) {
//                    System.out.println("unref内版本:"  + version);
                    DBUtil.dataBase.unref_am(version);
                    iterator.remove();
                }
            }
        }

        for (Map.Entry<String, HashMap<Integer, Integer>> oneWorkerOutVer: CacheUtil.workerOutVerRef.entrySet()) {
            String hostName = oneWorkerOutVer.getKey();
            HashMap<Integer, Integer> workerOutVersionMap = oneWorkerOutVer.getValue();
            Iterator<Map.Entry<Integer, Integer>> iterator = workerOutVersionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                int version = entry.getKey();
                int ref = entry.getValue();
                if (ref == 0) {
//                    System.out.println("unref外版本:"  + version);
                    DBUtil.dataBase.unref_st(version);
                    iterator.remove();
                }
            }
        }
    }


    private static void printWorkerVersion() {
        System.out.println("===============================");
        System.out.println("内版本");
        for (Map.Entry<String, HashMap<Integer, Integer>> oneWorkerInVer: CacheUtil.workerInVerRef.entrySet()) {
            String hostName = oneWorkerInVer.getKey();
            HashMap<Integer, Integer> workerInVersionMap = oneWorkerInVer.getValue();
            System.out.println(hostName + "内版本:");
            Iterator<Map.Entry<Integer, Integer>> iterator = workerInVersionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                int version = entry.getKey();
                int ref = entry.getValue();
                System.out.println("version: " + version + " ref:" + ref);
            }
        }

        System.out.println("外版本");
        for (Map.Entry<String, HashMap<Integer, Integer>> oneWorkerOutVer: CacheUtil.workerOutVerRef.entrySet()) {
            String hostName = oneWorkerOutVer.getKey();
            HashMap<Integer, Integer> workerOutVersionMap = oneWorkerOutVer.getValue();
            System.out.println(hostName + "外版本:");
            Iterator<Map.Entry<Integer, Integer>> iterator = workerOutVersionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                int version = entry.getKey();
                int ref = entry.getValue();
                System.out.println("version: " + version + " ref:" + ref);
            }
        }
        System.out.println("===============================\n");
    }
}
