package com.local.util;

import com.local.domain.Parameters;
import com.local.domain.Version;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheUtil {

    public static Version curVersion = null;   // 当前的版本
    public static Map<String, HashMap<Integer, Integer>> workerInVerRef = new ConcurrentHashMap<>(); // 所有worker的内存版本ref
    public static Map<String, HashMap<Integer, Integer>> workerOutVerRef = new ConcurrentHashMap<>(); // 所有worker的外存版本ref

    public static Map<Integer, MappedFileReaderBuffer> mappedFileReaderMapBuffer = new HashMap<>();
//    public static Map<Integer, MappedFileReader> mappedFileReaderMap = new HashMap<>();
    public static ExecutorService insertThreadPool = Executors.newFixedThreadPool(Parameters.insertNumThread + 1);
//    public static ExecutorService searchThreadPool = Executors.newFixedThreadPool(Parameters.numThread);
//
    public static class clearCache {
        public static void run() {
            String cmd = "sync && echo 1 > /proc/sys/vm/drop_caches";
            try {
                Process process = Runtime.getRuntime().exec(new String[] {"sudo", "-S", "sh", "-c", cmd});
                OutputStream outputStream = process.getOutputStream();
                outputStream.write(("45641146"+ "\n").getBytes());
                outputStream.write("sudo sh -c 'sync && echo 2 > /proc/sys/vm/drop_caches'\n".getBytes());
                outputStream.write("sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'\n".getBytes());
                outputStream.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
