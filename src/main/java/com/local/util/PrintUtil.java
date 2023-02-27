package com.local.util;

import com.local.domain.Parameters;
import com.local.search.SearchUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PrintUtil {
    public static void print(String str) {
        if (Parameters.debug) {
            System.out.println(str);
        }
    }
    public static void printTsBuffer(ByteBuffer tsBuffer) {
        if (!Parameters.debug)  return ;
        byte[] floatBytes = new byte[4];
        tsBuffer.rewind();
        for (int i = 0; i < Parameters.tsDataSize / 4; i ++ ) {
            tsBuffer.get(floatBytes);
            System.out.print(SearchUtil.bytesToFloat(floatBytes) + " ");
        }
        tsBuffer.rewind();
        System.out.println();
    }
    public static void printSaxTBuffer(ByteBuffer saxTBuffer) {
        if (!Parameters.debug)  return ;
        byte[] saxTBytes = new byte[Parameters.saxTSize];
        saxTBuffer.rewind();
        saxTBuffer.get(saxTBytes);
        System.out.print(Arrays.toString(saxTBytes));
        saxTBuffer.rewind();
        System.out.println();
    }
    public static void printSSTableBuffer(ByteBuffer ssTableBuffer) {
        if (!Parameters.debug)  return ;
        byte[] longBytes = new byte[8];
        ssTableBuffer.rewind();
        for (int i = 0; i < ssTableBuffer.capacity() / 8; i ++ ) {
            ssTableBuffer.get(longBytes);
            System.out.print(SearchUtil.bytesToLong(longBytes) + " ");
        }
        ssTableBuffer.rewind();
        System.out.println();
    }
    public static void printWorkerVersion() {
        if (!Parameters.debug)  return ;
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
