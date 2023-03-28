package com.cherry.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class FileUtil {
    private static final int readFileSize = 1024 * 100;

    // 获取文件夹下的所有文件
    public static ArrayList<File> getAllFile(String fileFolder) {

        File[] childrenFiles = new File(fileFolder).listFiles();
        if (childrenFiles == null) {
            throw new RuntimeException("文件夹不存在");
        }

        ArrayList<File> files = new ArrayList<>();
        for (File childFile : childrenFiles) {
            // 如果是文件，直接添加到结果集合
            if (childFile.isFile()) {
                files.add(childFile);
            }
        }
        return files;
    }
    public static void checkFileExists(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new RuntimeException(filePath + " not exists!");
        }
    }
    public static boolean createFolder(String folderPath) {
        File folder = new File(folderPath);
        if (!folder.exists()) {
            return folder.mkdir();
        }
        return false;
    }
    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) { // 有以前遗留的文件，删除后覆盖
            return file.delete();
        }
        return false;
    }
    public static void deleteFolderFiles(String folderPath) {
        ArrayList<File> files = FileUtil.getAllFile(folderPath);
        for (File file: files) {
            FileUtil.deleteFile(file.getPath());
        }
    }
}
