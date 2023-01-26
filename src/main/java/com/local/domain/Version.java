package com.local.domain;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;

public class Version {
    private HashMap<String, Pair<Integer, Integer>> workerVersions = new HashMap<>(); // key是hostName，value是内存和外存版本
    private int ref = 0;
    private RTree<String, Rectangle> rTree;

    public Version(RTree<String, Rectangle> rTree, HashMap<String, Pair<Integer, Integer>> workerVersions) {
        this.rTree = rTree;
        this.workerVersions = workerVersions;
    }

    public Version(RTree<String, Rectangle> rTree, HashMap<String, Pair<Integer, Integer>> workerVersions, int ref) {
        this.rTree = rTree;
        this.workerVersions = workerVersions;
        this.ref = ref;
    }


    public void updateVersion(String hostName, Pair<Integer, Integer> version) {
        workerVersions.put(hostName, version);
    }

    public void updateVersion(String hostName, Integer outVer) {
        Pair<Integer, Integer> oldVersion = workerVersions.get(hostName);
        Pair<Integer, Integer> newVersion = new Pair<>(oldVersion.getKey(), outVer);
        workerVersions.put(hostName, newVersion);
    }

    public HashMap<String, Pair<Integer, Integer>> getWorkerVersions() {
        return workerVersions;
    }


    public void addRef() {
        ref++ ;
    }
    public void unRef() {
        ref-- ;
    }

    public void setRef(int ref) {
        this.ref = ref;
    }

    public int getRef() {
        return ref;
    }

    public RTree<String, Rectangle> getrTree() {
        return rTree;
    }

    public void setrTree(RTree<String, Rectangle> rTree) {
        this.rTree = rTree;
    }

    public Version deepCopy() {

//        // 使用Gson序列化进行深拷贝
//        Gson gson = new Gson();
//        Version copyVersion = gson.fromJson(gson.toJson(this), Version.class);
//        return copyVersion;

        RTree<String, Rectangle> newRTree = this.rTree.delete("", Geometries.rectangle (-1, -1, -1, -1));

        HashMap<String, Pair<Integer, Integer>> newWorkerVersions = new HashMap<>();
        for(Map.Entry<String, Pair<Integer, Integer>> entry: workerVersions.entrySet()) {
            if (entry.getValue() != null) {
                newWorkerVersions.put(entry.getKey(), new Pair<>(entry.getValue().getKey(), entry.getValue().getValue()));
            }
            else {
                newWorkerVersions.put(entry.getKey(), null);
            }
        }

        return  new Version(newRTree, newWorkerVersions, this.ref);
    }

}
