package com.local.version;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.local.domain.Parameters;
import com.local.domain.Version;
import com.local.util.CacheUtil;
import com.local.version.VersionUtil;
import javafx.util.Pair;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class versionTest {
    @Test
    public void test1() {
        long l1 = 0x7fffffffffffffffL;
        System.out.println(l1 + " " + (double)l1);
        long l3 = 0x7ffffffffffffe00L;
        System.out.println(l3 + " " + (double)l3);
        long l4 = 0x7ffffffffffffdffL;
        System.out.println(l4 + " " + (double)l4);
        long l2 = 0x8000000000000000L;
        System.out.println(l2 + " " + (double)l2);

        boolean f = (double) l1 < (double) l3 ;
        System.out.println();

        byte[] a = {(byte)0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        System.out.println(VersionUtil.saxToDouble(a));
        byte[] b = {(byte)0xff, (byte) 0x00, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        System.out.println(VersionUtil.saxToDouble(b));
        byte[] c = {(byte)0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        System.out.println(VersionUtil.saxToDouble(c));
        byte[] d = {(byte)0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        System.out.println(VersionUtil.saxToDouble(d));
    }

    @Test
    public void rTreeSaxValueTest() {
        byte[] maxSax = new byte[]{(byte)255, (byte)255, (byte)255, (byte)255, (byte) 255, (byte) 255, (byte) 255, (byte) 255};
        System.out.println(Arrays.toString(maxSax));
        byte[] minSax = new byte[]{(byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0};
        System.out.println(Arrays.toString(minSax));
        String nodeValue = new String(maxSax, StandardCharsets.ISO_8859_1) + new String(minSax, StandardCharsets.ISO_8859_1) + new String(new byte[8], StandardCharsets.ISO_8859_1) + Parameters.hostName;
        System.out.println("");
        RTree<String, Rectangle> tree = RTree.create();
        tree = tree.add(nodeValue, Geometries.rectangle(3, 8, 25, 32));
        Iterable<Entry<String, Rectangle>> results = tree.search(Geometries.rectangle(-Double.MAX_VALUE,-Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE)).toBlocking().toIterable();
        for (Entry<String, Rectangle> result : results) {
            String value = result.value();
            System.out.println(value.length());
            String maxSaxStr = value.substring(0, Parameters.saxDataSize);
            System.out.println(Arrays.toString(maxSaxStr.getBytes(StandardCharsets.ISO_8859_1)));
//            System.out.println(hostName);
        }
    }

    @Test
    public void testStringByte() {
//        byte[] maxSax = new byte[]{(byte)255, (byte)255, (byte)255, (byte)255, (byte) 255, (byte) 255, (byte) 255, (byte) 255};
        long t1 = System.currentTimeMillis();

        for (int i = 0; i <= 255; i ++ ) {
            int a = i % 256;
            byte[] maxSax = new byte[]{(byte)a, (byte)a, (byte)a, (byte)a, (byte)a, (byte)a, (byte)a, (byte)a};
//            System.out.println(Arrays.toString(maxSax));
            String maxSaxStr = new String(maxSax, StandardCharsets.ISO_8859_1);
//            System.out.println(maxSaxStr);
            System.out.println(Arrays.toString(maxSaxStr.getBytes(StandardCharsets.ISO_8859_1)));
//            System.out.println(Arrays.toString(maxSax1));
//            System.out.println();
        }
        System.out.println(System.currentTimeMillis() - t1);
    }

    @Test
    public void rTreeTest() {
        RTree<String, Rectangle> tree = RTree.create();

        tree = tree.add(Parameters.hostName + ":"  + "1", Geometries.rectangle(3, 8, 25, 32));
        tree = tree.add(Parameters.hostName + ":"  + "3", Geometries.rectangle(4, 23, 12, 29));
        tree = tree.add(Parameters.hostName + ":"  + "6", Geometries.rectangle(7, 5, 13, 13));
        tree = tree.add(Parameters.hostName + ":"  + "7", Geometries.rectangle(8, 6, 12, 12));

        Iterable<Entry<String, Rectangle>> results = tree.search(Geometries.rectangle(Double.MIN_VALUE,Double.MIN_VALUE,Double.MAX_VALUE,Double.MAX_VALUE)).toBlocking().toIterable();
        ArrayList<Long> sstableNumList = new ArrayList<>();
//        Iterable<Entry<String, Rectangle>> results = tree.entries().toBlocking().toIterable();
        for (Entry<String, Rectangle> result : results) {
            System.out.println(result);

//            String[] str = result.value().split(":");
//            System.out.println(str[1]);
//            sstableNumList.add(Long.valueOf(str[1]));
        }

        System.out.println();
        tree = tree.delete(Parameters.hostName + ":"  + "7", Geometries.rectangle(8, 6, 12, 12));
        results = tree.entries().toBlocking().toIterable();
        for (Entry<String, Rectangle> result : results) {
            System.out.println(result);
        }
//        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();
//        System.out.println(Arrays.toString(sstableNum));
//        results = tree.search(Geometries.rectangle(7,5,13,7)).toBlocking().toIterable();
//        for (Entry<String, Rectangle> result : results) {
//            System.out.println(result);
//            System.out.println(result.value());
//        }
    }
    @Test
    public void rTreeTestDel() {
        RTree<byte[], Rectangle> tree = RTree.create();

        tree = tree.add(new byte[]{1}, Geometries.rectangle(3, 8, 25, 32));
        tree = tree.add(new byte[]{2}, Geometries.rectangle(4, 23, 12, 29));
        tree = tree.add(new byte[]{3}, Geometries.rectangle(7, 5, 13, 13));
        tree = tree.add(new byte[]{}, Geometries.rectangle(8, 6, 12, 12));

        Iterable<Entry<byte[], Rectangle>> results = tree.search(Geometries.rectangle(Double.MIN_VALUE,Double.MIN_VALUE,Double.MAX_VALUE,Double.MAX_VALUE)).toBlocking().toIterable();
        ArrayList<Long> sstableNumList = new ArrayList<>();
//        Iterable<Entry<String, Rectangle>> results = tree.entries().toBlocking().toIterable();
        for (Entry<byte[], Rectangle> result : results) {
            System.out.println(result);

//            String[] str = result.value().split(":");
//            System.out.println(str[1]);
//            sstableNumList.add(Long.valueOf(str[1]));
        }

        System.out.println();
        tree = tree.delete(new byte[]{3}, Geometries.rectangle(8, 6, 12, 12));
        results = tree.entries().toBlocking().toIterable();
        for (Entry<byte[], Rectangle> result : results) {
            System.out.println(result);
        }
//        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();
//        System.out.println(Arrays.toString(sstableNum));
//        results = tree.search(Geometries.rectangle(7,5,13,7)).toBlocking().toIterable();
//        for (Entry<String, Rectangle> result : results) {
//            System.out.println(result);
//            System.out.println(result.value());
//        }
    }

    @Test
    public void rTreeTest1() {
        RTree<String, Rectangle> tree = RTree.create();

//        tree = tree.add(Parameters.hostName + ":"  + "1", Geometries.rectangle(-7.2049724604355024E16, 0.0, 9.1333006301018563E18, 0.0));
        tree = tree.add(Parameters.hostName + ":"  + "3", Geometries.rectangle(-7, 0.0, 9, 1));
//        tree = tree.add(Parameters.hostName + ":"  + "6", Geometries.rectangle(-9.1154617147946025E18, 0.0, -7.9938815400873984E16, 0.0));
//        tree = tree.add(Parameters.hostName + ":"  + "7", Geometries.rectangle(-9.1154617147946025E18, 0.0, -7.9938815400873984E16, 1.67403219458E9));
        tree = tree.add(Parameters.hostName + ":"  + "7", Geometries.rectangle(-9, -100, -1, 100));

        Iterable<Entry<String, Rectangle>> results = tree.search(Geometries.rectangle(-Double.MAX_VALUE,-Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE)).toBlocking().toIterable();

//        Iterable<Entry<String, Rectangle>> results = tree.search(Geometries.rectangle(-3,Double.MIN_VALUE,Double.MAX_VALUE,Double.MAX_VALUE)).toBlocking().toIterable();
        ArrayList<Long> sstableNumList = new ArrayList<>();
//        Iterable<Entry<String, Rectangle>> results = tree.entries().toBlocking().toIterable();
        for (Entry<String, Rectangle> result : results) {
            System.out.println(result);

//            String[] str = result.value().split(":");
//            System.out.println(str[1]);
//            sstableNumList.add(Long.valueOf(str[1]));
        }
//        long[] sstableNum = sstableNumList.stream().mapToLong(num -> num).toArray();
//        System.out.println(Arrays.toString(sstableNum));
//        results = tree.search(Geometries.rectangle(7,5,13,7)).toBlocking().toIterable();
//        for (Entry<String, Rectangle> result : results) {
//            System.out.println(result);
//            System.out.println(result.value());
//        }
    }

    @Test
    public void deepCopyTest() {
        RTree<String, Rectangle> tree = RTree.create();

        tree = tree.add("1", Geometries.rectangle(3, 8, 25, 32));
        tree = tree.add("2", Geometries.rectangle(4, 23, 12, 29));
        tree = tree.add("3", Geometries.rectangle(7, 5, 13, 13));

        HashMap<String, Pair<Integer, Integer>> workerVersions = new HashMap<>();
        workerVersions.put(Parameters.hostName, new Pair<>(0, 0));
        Version version = new Version(tree, workerVersions);

        version.setRef(1);
        CacheUtil.curVersion = version;

        Version newVersion = CacheUtil.curVersion.deepCopy();
        System.out.println(newVersion.getrTree());
        version.setrTree(null);
        System.out.println(newVersion.getrTree());
    }

    @Test
    public void myTest() {
        double a = -4.6723896417293199E18;
        double b = 1.44054218488241632E17;
        System.out.println(b >= a);
        Geometries.rectangle (a, b, 0, 0);
//        Geometries.rectangle (-1, 0, 0, 0);
    }

    @Test
    public void testSpeed() {
//        byte[] a = new byte[(int)1e9];
//        byte[] b = new byte[(int)1e9];
        long t1 = System.currentTimeMillis();
//        for (int i = 0; i < 1e6; i ++){
//            VersionUtil.bytesToLong(new byte[8]);
//        }
//        System.arraycopy(b, 0, a, 0, a.length);
        byte[] a = new byte[(int)1e3];
        byte[] b = new byte[(int)1e3];
        System.out.println(System.currentTimeMillis() - t1);

    }

}
