package com.local.util;

import com.local.domain.Sax;
import com.local.domain.TimeSeries;
import com.local.domain.Version;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class CacheUtil {

    public static Version curVersion = null;   // 当前的版本
    public static Map<String, HashMap<Integer, Integer>> workerInVerRef = new ConcurrentHashMap<>(); // 所有worker的内存版本ref
    public static Map<String, HashMap<Integer, Integer>> workerOutVerRef = new ConcurrentHashMap<>(); // 所有worker的外存版本ref

    public static Map<Integer, FileChannelReader> fileChannelReaderMap = new ConcurrentHashMap<>();
}
