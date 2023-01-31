package com.local.util;

import com.local.domain.Parameters;
import com.local.domain.Version;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CacheUtil {

    public static Version curVersion = null;   // 当前的版本
    public static Map<String, HashMap<Integer, Integer>> workerInVerRef = new ConcurrentHashMap<>(); // 所有worker的内存版本ref
    public static Map<String, HashMap<Integer, Integer>> workerOutVerRef = new ConcurrentHashMap<>(); // 所有worker的外存版本ref

//    public static Map<Integer, FileChannelReader> fileChannelReaderMap = new HashMap<>();
    public static Map<Integer, MappedFileReader> mappedFileReaderMap = new HashMap<>();

    public static ExecutorService insertThreadPool = Executors.newFixedThreadPool(Parameters.numThread);
    public static ExecutorService searchThreadPool = Executors.newFixedThreadPool(Parameters.numThread);
}
