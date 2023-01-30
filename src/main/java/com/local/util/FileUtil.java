package com.local.util;

import com.local.domain.Parameters;
import com.local.domain.TimeSeries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
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

    public static TimeSeries readTsFromFIle(String fileFolder, long readPosition) throws IOException {
        File file = new File(fileFolder + "/" + "ts.dat");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");//r: 只读模式 rw:读写模式
        randomAccessFile.seek(readPosition);
        byte[] tsDataBytes = new byte[Parameters.timeSeriesDataSize];
        int readSize = randomAccessFile.read(tsDataBytes);
        if (readSize <= 0) {
            throw new RuntimeException("读取错误");
        }
        randomAccessFile.seek(readPosition + Parameters.timeSeriesDataSize);
        byte[] timeStampBytes = new byte[Parameters.timeStampSize];
        readSize = randomAccessFile.read(timeStampBytes);
        if (readSize <= 0) {
            throw new RuntimeException("读取错误");
        }
        randomAccessFile.close();

        return new TimeSeries(tsDataBytes, timeStampBytes);
    }


    public static long writeFile(String fileFolder, TimeSeries timeSeries) throws IOException {
        createFolder(fileFolder);
//        File file = new File(fileFolder + "/" + TsUtil.computeHash(timeSeries));
        File file = new File(fileFolder + "/" + "ts.dat");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");//r: 只读模式 rw:读写模式


        long pos = randomAccessFile.length();
            long t3 = System.currentTimeMillis();
        randomAccessFile.seek(pos);      //移动文件记录指针的位置,
        randomAccessFile.write(timeSeries.getTimeSeriesData());        //调用了seek（start）方法，是指把文件的记录指针定位到start字节的位置。也就是说程序将从start字节开始写数据
        randomAccessFile.seek(pos + timeSeries.getTimeSeriesData().length);
        randomAccessFile.write(timeSeries.getTimeStamp());

        randomAccessFile.close();

        return pos;
    }

    public static long writeTsLock(String fileFolder, TimeSeries timeSeries) {
            long t1 = System.currentTimeMillis();
        File folder = new File(fileFolder);
        if (!folder.exists()) {
            boolean flag = folder.mkdir();
        }
//        File file = new File(fileFolder + "/" + TsUtil.computeHash(timeSeries));
        File file = new File(fileFolder + "/" + "ts.dat");
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");//r: 只读模式 rw:读写模式
            FileChannel channel = randomAccessFile.getChannel();
            FileLock fileLock;
            while (true) {
                try {
                    fileLock = channel.tryLock();
                    if (fileLock != null) {
                        break;
                    }
                } catch (Exception e) {
                    Thread.sleep(0);    // 马上加入到可执行队列
                }
            }

            long pos = randomAccessFile.length();

            randomAccessFile.seek(pos);      //移动文件记录指针的位置,
            randomAccessFile.write(timeSeries.getTimeSeriesData());        //调用了seek（start）方法，是指把文件的记录指针定位到start字节的位置。也就是说程序将从start字节开始写数据
            randomAccessFile.seek(pos + timeSeries.getTimeSeriesData().length);
            randomAccessFile.write(timeSeries.getTimeStamp());

            fileLock.release();
            randomAccessFile.close();

            return pos;
        }
        catch (Exception e) {
            throw new RuntimeException("写文件异常");
        }
    }
    public static RandomAccessFile writeTsInit(String fileFolder) {
        File folder = new File(fileFolder);
        if (!folder.exists()) {
            boolean flag = folder.mkdir();
        }

        File file = new File(fileFolder + "/" + "ts.dat");
        try {
            return new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("写文件初始化错误" + e);
        }
    }
    public static long writeTs(TimeSeries timeSeries, RandomAccessFile randomAccessFile) throws IOException {
        long pos = randomAccessFile.length();
//            long t3 = System.currentTimeMillis();
        randomAccessFile.seek(pos);      //移动文件记录指针的位置,
        randomAccessFile.write(timeSeries.getTimeSeriesData());        //调用了seek（start）方法，是指把文件的记录指针定位到start字节的位置。也就是说程序将从start字节开始写数据
        randomAccessFile.seek(pos + timeSeries.getTimeSeriesData().length);
        randomAccessFile.write(timeSeries.getTimeStamp());
//            Main.writeTime += System.currentTimeMillis() - t3;
        return pos;
    }

    public static void writeTsFinish(RandomAccessFile randomAccessFile) {
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            throw new RuntimeException("关闭RandomAccessFile错误" + e);
        }
    }





}
