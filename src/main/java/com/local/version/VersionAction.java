package com.local.version;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.domain.Parameters;
import com.local.domain.Version;
import com.local.util.CacheUtil;
import com.local.util.DBUtil;
import com.local.domain.Pair;
import com.local.util.PrintUtil;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class VersionAction {
    public static synchronized void unRefVersion(Version version) {
        PrintUtil.print("unRef大版本");

        version.unRef();
        if (version.getRef() == 0) {
            HashMap<String, Pair<Integer, Integer>> workerVersions = version.getWorkerVersions();
            unRefWorkerVersions(workerVersions);
        }
        PrintUtil.printWorkerVersion();
    }
    public static synchronized void refVersion(Version version) {
        PrintUtil.print("ref大版本");
        version.addRef();
        PrintUtil.printWorkerVersion();
    }

    private static synchronized void unRefWorkerVersions(HashMap<String, Pair<Integer, Integer>> workerVersions) {
//        System.out.println("开始unRef小版本");
        for (Map.Entry<String, Pair<Integer, Integer>> entry: workerVersions.entrySet()) {
            String hostName = entry.getKey();
            if (entry.getValue() == null) continue; // db第一次发送版本时,还没有小版本

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

    public static synchronized void initVersion() {
        RTree<String, Rectangle> tree = RTree.create();
        HashMap<String, Pair<Integer, Integer>> workerVersions = new HashMap<>();
        workerVersions.put(Parameters.hostName, null);
        Version version = new Version(tree, workerVersions);
        version.setRef(1);

        CacheUtil.curVersion = version;
    }

//    public static synchronized void createNewVerFromOldVerCopy(byte[] versionBytes, String workerHostName) {
//        Version newVersion = CacheUtil.curVersion.deepCopy();
//        if (versionBytes[0] == 0) {
//            VersionUtil.Version1Content ver1 = new VersionUtil.Version1Content();
//
//            VersionUtil.analysisVersionBytes(versionBytes, ver1);
//
//            PrintUtil.print("\t需要更新版本1 copy:" + "内" + ver1.inVer + " 外" + ver1.outVer + " 文件"
//                    + ver1.fileNum + " " + ver1.minTime + " " + ver1.maxTime);
//            newVersion.updateVersion(workerHostName, new Pair<>(ver1.inVer, ver1.outVer));
//
//
//            // rtree插入
//            RTree<String, Rectangle> tree = newVersion.getrTree();
//            PrintUtil.print("\trtree插入:" + VersionUtil.saxT2Double(ver1.minSax) + " " + VersionUtil.saxT2Double(ver1.maxSax) + " " + ver1.minTime + " " + ver1.maxTime + " 文件名" + ver1.fileNum);
//
//            tree = tree.add(new String(ver1.minSax, StandardCharsets.ISO_8859_1) + new String(ver1.maxSax, StandardCharsets.ISO_8859_1) + ver1.fileNum,
//                    Geometries.rectangle (VersionUtil.saxT2Double(ver1.minSax), (double) ver1.minTime, VersionUtil.saxT2Double(ver1.maxSax), (double) ver1.maxTime));
//
//            newVersion.setrTree(tree);
//            PrintUtil.print("\trtree插入完成");
//
//            newVersion.setRef(1);    // 大版本ref=1
//            refWorkerVersions(newVersion.getWorkerVersions());  // 该大版本包括所有worker的小版本ref+1
//
//            PrintUtil.print("\t当前版本ref" + CacheUtil.curVersion.getRef());
//            unRefVersion(CacheUtil.curVersion);  // 上一个大版本ref-1
//            CacheUtil.curVersion = newVersion; // 新版本覆盖旧版本
//
//            PrintUtil.print("\t更新完成 ");
//            PrintUtil.printWorkerVersion();
//        }
//        else if (versionBytes[0] == 1) {
//            VersionUtil.Version2Content ver2 = new VersionUtil.Version2Content();
//            VersionUtil.analysisVersionBytes(versionBytes, ver2);
//            PrintUtil.print("\t需要更新版本2 copy:" + " 外" + ver2.outVer + " 文件" + ver2.addFileNums + " " + ver2.delFileNums);
//
//            newVersion.updateVersion(workerHostName, ver2.outVer);
//
//            PrintUtil.print("\trtree插入:" + "时间" + ver2.addMinTimes + " " + ver2.addMaxTimes + " 文件名" + ver2.addFileNums);
//            // rtree删除
//            RTree<String, Rectangle> tree = newVersion.getrTree();
//            for (int i = 0; i < ver2.delMaxSaxes.size(); i ++ ) {
//                tree = tree.delete(new String(ver2.delMinSaxes.get(i), StandardCharsets.ISO_8859_1) + new String(ver2.delMaxSaxes.get(i), StandardCharsets.ISO_8859_1) + ver2.delFileNums.get(i),
//                        Geometries.rectangle (VersionUtil.saxT2Double(ver2.delMinSaxes.get(i)), (double) ver2.delMinTimes.get(i), VersionUtil.saxT2Double(ver2.delMaxSaxes.get(i)), (double) ver2.delMaxTimes.get(i)));
//            }
//
//            // rtree增加
//            for (int i = 0; i < ver2.addMaxSaxes.size(); i ++ ) {
//                tree = tree.add(new String(ver2.addMinSaxes.get(i), StandardCharsets.ISO_8859_1) + new String(ver2.addMaxSaxes.get(i), StandardCharsets.ISO_8859_1) + ver2.addFileNums.get(i),
//                        Geometries.rectangle (VersionUtil.saxT2Double(ver2.addMinSaxes.get(i)), (double) ver2.addMinTimes.get(i), VersionUtil.saxT2Double(ver2.addMaxSaxes.get(i)), (double) ver2.addMaxTimes.get(i)));
//            }
//            newVersion.setrTree(tree);
//
//            newVersion.setRef(1);
//            refWorkerVersions(newVersion.getWorkerVersions());
//            unRefVersion(CacheUtil.curVersion);
//            CacheUtil.curVersion = newVersion; // 新版本覆盖旧版本
//            PrintUtil.print("\t更新完成 ");
//            PrintUtil.printWorkerVersion();
//        } else {
//            throw new RuntimeException("版本错误");
//        }
//    }
//
//    public static synchronized void createNewVerFromOldVer(byte[] versionBytes, String workerHostName) {
////        System.out.println("---------------------------------");
//        if (versionBytes[0] == 0) {
//            VersionUtil.Version1Content ver1 = new VersionUtil.Version1Content();
//
//            VersionUtil.analysisVersionBytes(versionBytes, ver1);
//
//            PrintUtil.print("\t需要更新版本1:" + "内" + ver1.inVer + " 外" + ver1.outVer + " 文件" + ver1.fileNum + " 时间：" + ver1.minTime + " " + ver1.maxTime);
//
//            unRefVersion(CacheUtil.curVersion);  // 当前大版本ref-1,先清除小版本再加入新的小版本
//            CacheUtil.curVersion.updateVersion(workerHostName, new Pair<>(ver1.inVer, ver1.outVer));
//
//            // rtree插入
//            RTree<String, Rectangle> tree = CacheUtil.curVersion.getrTree();
//            PrintUtil.print("\trtree插入:" + VersionUtil.saxT2Double(ver1.minSax) + " " + VersionUtil.saxT2Double(ver1.maxSax) + " 时间：" + ver1.minTime + " " + ver1.maxTime + " 文件名" + ver1.fileNum);
//            tree = tree.add(new String(ver1.minSax, StandardCharsets.ISO_8859_1) + new String(ver1.maxSax, StandardCharsets.ISO_8859_1) + ver1.fileNum,
//                    Geometries.rectangle (VersionUtil.saxT2Double(ver1.minSax), (double) ver1.minTime, VersionUtil.saxT2Double(ver1.maxSax), (double) ver1.maxTime));
//
//            CacheUtil.curVersion.setrTree(tree);
//            PrintUtil.print("\trtree插入完成");
//
//            CacheUtil.curVersion.setRef(1);    // 大版本ref=1
//
//            refWorkerVersions(CacheUtil.curVersion.getWorkerVersions());  // 该大版本包括所有worker的小版本ref+1
//            PrintUtil.print("\t当前版本ref" + CacheUtil.curVersion.getRef());
//            PrintUtil.print("\t更新完成 ");
//            PrintUtil.printWorkerVersion();
//        }
//        else if (versionBytes[0] == 1) {
//            VersionUtil.Version2Content ver2 = new VersionUtil.Version2Content();
//            VersionUtil.analysisVersionBytes(versionBytes, ver2);
//            PrintUtil.print("\t需要更新版本2:" + " 外" + ver2.outVer + " 文件" + ver2.addFileNums + " " + ver2.delFileNums);
//
//            unRefVersion(CacheUtil.curVersion);  // 上一个大版本ref-1
//            CacheUtil.curVersion.updateVersion(workerHostName, ver2.outVer);
//
//            // rtree删除
//            RTree<String, Rectangle> tree = CacheUtil.curVersion.getrTree();
//            for (int i = 0; i < ver2.delMaxSaxes.size(); i ++ ) {
//                tree = tree.delete(new String(ver2.delMinSaxes.get(i), StandardCharsets.ISO_8859_1)  + new String(ver2.delMaxSaxes.get(i), StandardCharsets.ISO_8859_1) + ver2.delFileNums.get(i),
//                        Geometries.rectangle (VersionUtil.saxT2Double(ver2.delMinSaxes.get(i)), (double) ver2.delMinTimes.get(i), VersionUtil.saxT2Double(ver2.delMaxSaxes.get(i)), (double) ver2.delMaxTimes.get(i)));
//            }
//            PrintUtil.print("\trtree插入:" + "时间" + ver2.addMinTimes + " " + ver2.addMaxTimes + " 文件名" + ver2.addFileNums);
//            // rtree增加
//            for (int i = 0; i < ver2.addMaxSaxes.size(); i ++ ) {
//                PrintUtil.print(Arrays.toString(ver2.addMinSaxes.get(i)) + " " + Arrays.toString(ver2.addMaxSaxes.get(i)) + " " +
//                        VersionUtil.bytesToLong(ver2.addMinSaxes.get(i)) + " " + VersionUtil.bytesToLong(ver2.addMaxSaxes.get(i)) +
//                        VersionUtil.saxT2Double(ver2.addMinSaxes.get(i)) + " " + VersionUtil.saxT2Double(ver2.addMaxSaxes.get(i)));
//                tree = tree.add(new String(ver2.addMinSaxes.get(i), StandardCharsets.ISO_8859_1)  + new String(ver2.addMaxSaxes.get(i), StandardCharsets.ISO_8859_1) + ver2.addFileNums.get(i),
//                        Geometries.rectangle (VersionUtil.saxT2Double(ver2.addMinSaxes.get(i)), (double) ver2.addMinTimes.get(i), VersionUtil.saxT2Double(ver2.addMaxSaxes.get(i)), (double) ver2.addMaxTimes.get(i)));
//            }
//            CacheUtil.curVersion.setrTree(tree);
//
//            CacheUtil.curVersion.setRef(1);
//
//            refWorkerVersions(CacheUtil.curVersion.getWorkerVersions());
//            PrintUtil.print("\t更新完成 ");
//            PrintUtil.printWorkerVersion();
//
//        } else {
//            throw new RuntimeException("版本错误");
//        }
//    }

    public static synchronized void createNewVer(byte[] versionBytes, String workerHostName, boolean isCopy) {
//        System.out.println("---------------------------------");
        Version newVersion;
        if(isCopy) {
            newVersion = CacheUtil.curVersion.deepCopy();
        }
        else {
            newVersion = CacheUtil.curVersion;
        }
        if (versionBytes[0] == 0) {
            VersionUtil.Version1Content ver1 = new VersionUtil.Version1Content();

            VersionUtil.analysisVersionBytes(versionBytes, ver1);

            PrintUtil.print("\t需要更新版本1:" + "内" + ver1.inVer + " 外" + ver1.outVer + " 文件" + ver1.fileNum + " 时间：" + ver1.minTime + " " + ver1.maxTime);

            unRefVersion(CacheUtil.curVersion);  // 旧的大版本ref-1,先清除小版本再加入新的小版本
            newVersion.updateVersion(workerHostName, new Pair<>(ver1.inVer, ver1.outVer));

            // rtree插入
            RTree<String, Rectangle> tree = newVersion.getrTree();
            PrintUtil.print("\trtree插入:" + VersionUtil.saxT2Double(ver1.minSax) + " " + VersionUtil.saxT2Double(ver1.maxSax) + " 时间：" + ver1.minTime + " " + ver1.maxTime + " 文件名" + ver1.fileNum);
            tree = tree.add(new String(ver1.minSax, StandardCharsets.ISO_8859_1) + new String(ver1.maxSax, StandardCharsets.ISO_8859_1) + ver1.fileNum,
                    Geometries.rectangle (VersionUtil.saxT2Double(ver1.minSax), (double) ver1.minTime, VersionUtil.saxT2Double(ver1.maxSax), (double) ver1.maxTime));

            newVersion.setrTree(tree);
            PrintUtil.print("\trtree插入完成");

            newVersion.setRef(1);    // 大版本ref=1

            refWorkerVersions(newVersion.getWorkerVersions());  // 该大版本包括所有worker的小版本ref+1
            if (isCopy) {
                CacheUtil.curVersion = newVersion;
            }
            PrintUtil.print("\t当前版本ref" + CacheUtil.curVersion.getRef());
            PrintUtil.print("\t更新完成 ");
            PrintUtil.printWorkerVersion();
        }
        else if (versionBytes[0] == 1) {
            VersionUtil.Version2Content ver2 = new VersionUtil.Version2Content();
            VersionUtil.analysisVersionBytes(versionBytes, ver2);
            PrintUtil.print("\t需要更新版本2:" + " 外" + ver2.outVer + " 文件" + ver2.addFileNums + " " + ver2.delFileNums);

            unRefVersion(CacheUtil.curVersion);   // 旧的大版本ref-1
            newVersion.updateVersion(workerHostName, ver2.outVer);

            // rtree删除
            RTree<String, Rectangle> tree = newVersion.getrTree();
            for (int i = 0; i < ver2.delMaxSaxes.size(); i ++ ) {
                tree = tree.delete(new String(ver2.delMinSaxes.get(i), StandardCharsets.ISO_8859_1)  + new String(ver2.delMaxSaxes.get(i), StandardCharsets.ISO_8859_1) + ver2.delFileNums.get(i),
                        Geometries.rectangle (VersionUtil.saxT2Double(ver2.delMinSaxes.get(i)), (double) ver2.delMinTimes.get(i), VersionUtil.saxT2Double(ver2.delMaxSaxes.get(i)), (double) ver2.delMaxTimes.get(i)));
            }
            PrintUtil.print("\trtree插入:" + "时间" + ver2.addMinTimes + " " + ver2.addMaxTimes + " 文件名" + ver2.addFileNums);
            // rtree增加
            for (int i = 0; i < ver2.addMaxSaxes.size(); i ++ ) {
//                PrintUtil.print(Arrays.toString(ver2.addMinSaxes.get(i)) + " " + Arrays.toString(ver2.addMaxSaxes.get(i)) + " " +
//                        VersionUtil.bytesToLong(ver2.addMinSaxes.get(i)) + " " + VersionUtil.bytesToLong(ver2.addMaxSaxes.get(i)) +
//                        VersionUtil.saxT2Double(ver2.addMinSaxes.get(i)) + " " + VersionUtil.saxT2Double(ver2.addMaxSaxes.get(i)));
                tree = tree.add(new String(ver2.addMinSaxes.get(i), StandardCharsets.ISO_8859_1)  + new String(ver2.addMaxSaxes.get(i), StandardCharsets.ISO_8859_1) + ver2.addFileNums.get(i),
                        Geometries.rectangle (VersionUtil.saxT2Double(ver2.addMinSaxes.get(i)), (double) ver2.addMinTimes.get(i), VersionUtil.saxT2Double(ver2.addMaxSaxes.get(i)), (double) ver2.addMaxTimes.get(i)));
            }
            newVersion.setrTree(tree);

            newVersion.setRef(1);

            refWorkerVersions(newVersion.getWorkerVersions());
            if (isCopy) {
                CacheUtil.curVersion = newVersion;
            }
            PrintUtil.print("\t更新完成 ");
            PrintUtil.printWorkerVersion();

        } else {
            throw new RuntimeException("版本错误");
        }
    }

    public static void changeVersion(byte[] versionBytes, String workerHostName) {
        if (CacheUtil.curVersion.getRef() > 1){    // 有查询正在使用版本或db第一次发来版本
            PrintUtil.print("开始更新版本 有查询");
//            createNewVerFromOldVerCopy(versionBytes, workerHostName);
            createNewVer(versionBytes, workerHostName, true);
        }
        else if (CacheUtil.curVersion.getRef() == 1) {   // 没有查询正在使用版本
            PrintUtil.print("开始更新版本 没有查询");
//            createNewVerFromOldVer(versionBytes, workerHostName);
            createNewVer(versionBytes, workerHostName, false);
        }
        else {
//            PrintUtil.print("开始更新版本 ref=0 大版本ref错误");
            System.out.println("开始更新版本 ref=0 大版本ref错误");
            throw new RuntimeException("大版本ref错误");
        }
    }

    public static synchronized void checkWorkerVersion() {   // 检查worker的小版本的ref，如果=0则通知worker删除版本
        for (Map.Entry<String, HashMap<Integer, Integer>> oneWorkerInVer: CacheUtil.workerInVerRef.entrySet()) {    // 一个worker的版本
            String hostName = oneWorkerInVer.getKey();
            HashMap<Integer, Integer> workerInVersionMap = oneWorkerInVer.getValue();
            Iterator<Map.Entry<Integer, Integer>> iterator = workerInVersionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                int version = entry.getKey();
                int ref = entry.getValue();
                if (ref == 0) {
                    PrintUtil.print("unref内版本:"  + version);
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
                    PrintUtil.print("unref外版本:"  + version);
                    DBUtil.dataBase.unref_st(version);
                    iterator.remove();
                }
            }
        }
    }

}
