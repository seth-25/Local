package com.local.version;

import com.local.domain.Parameters;
import com.local.util.CacheUtil;
import com.local.util.FileUtil;
import com.local.util.MappedFileReader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class readTest {
    @Test
    public void readSpeedTest100() throws IOException {
        Random random = new Random();
        ArrayList<Integer> list = new ArrayList<>();
        int num = 256;

        for (int i = 0; i < num; i ++ ) {
            list.add(random.nextInt((int)1e8));
        }
        Collections.sort(list);

        System.out.println(list);
        File file = new File(Parameters.FileSetting.inputPath + "100.bin");
        MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, 1);  // 初始化的ts
        long readTime = 0;
        byte[] ts;
        for (int i = 0; i < num; i ++ ) {
            long t2 = System.currentTimeMillis();   // todo
            ts = reader.readTs(list.get(i));
            readTime += (System.currentTimeMillis() - t2);   // todo
        }
        System.out.println(readTime);
    }

    @Test
    public void readSpeedTest30() throws IOException {
        Random random = new Random();
        ArrayList<Integer> list = new ArrayList<>();
        int num = 256;
        for (int i = 0; i < num; i ++ ) {
            list.add(random.nextInt((int)3e7));
        }
        Collections.sort(list);
        System.out.println(list);
        File file = new File(Parameters.FileSetting.inputPath + "30.bin");
        MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, 1);  // 初始化的ts
        long readTime = 0;
        byte[] ts;
        long t2 = System.currentTimeMillis();   // todo
        for (int i = 0; i < num; i ++ ) {
            ts = reader.readTs(list.get(i));
        }
        readTime += (System.currentTimeMillis() - t2);   // todo
        System.out.println(readTime);
    }

    @Test
    public void readSpeedTest10() throws IOException {
        Random random = new Random();
        ArrayList<Integer> list = new ArrayList<>();
        int num = 256;
        for (int i = 0; i < num; i ++ ) {
            list.add(random.nextInt((int)1e7));
        }
        Collections.sort(list);
        System.out.println(list);
        File file = new File(Parameters.FileSetting.inputPath + "10.bin");
        MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, 1);  // 初始化的ts
        long readTime = 0;
        byte[] ts;
        long t2 = System.currentTimeMillis();   // todo
        for (int i = 0; i < num; i ++ ) {
            ts = reader.readTs(list.get(i));
        }
        readTime += (System.currentTimeMillis() - t2);   // todo
        System.out.println(readTime);
    }


    @Test
    public void readSpeedTest1() throws IOException {
        Random random = new Random();
        ArrayList<Integer> list = new ArrayList<>();
        int num = 256;
        for (int i = 0; i < num; i ++ ) {
            list.add(random.nextInt((int)1e6));
        }
        Collections.sort(list);
        System.out.println(list);
        File file = new File(Parameters.FileSetting.inputPath + "1.bin");
        MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, 1);  // 初始化的ts
        long readTime = 0;
        byte[] ts;
        long t2 = System.currentTimeMillis();   // todo
        for (int i = 0; i < num; i ++ ) {
            ts = reader.readTs(list.get(i));
        }
        readTime += (System.currentTimeMillis() - t2);   // todo
        System.out.println(readTime);
    }



    @Test
    public void readSpeedRTest100() throws IOException {
        Random random = new Random();
        ArrayList<Long> list = new ArrayList<>();
        int num = 256;
        for (int i = 0; i < num; i ++ ) {
            list.add((long)random.nextInt((int)1e8));
        }
        Collections.sort(list);
        System.out.println(list);

        RandomAccessFile randomAccessFile = new RandomAccessFile(Parameters.FileSetting.inputPath + "100.bin", "r");//r: 只读模式 rw:读写模式

        long readTime = 0;
        byte[] ts = new byte[Parameters.tsSize];
        long t2 = System.currentTimeMillis();   // todo
        for (int i = 0; i < num; i ++ ) {
//            System.out.println(list.get(i) * Parameters.tsSize);
            randomAccessFile.seek(list.get(i) * Parameters.tsSize);

            int readSize = randomAccessFile.read(ts);
        }
        readTime += (System.currentTimeMillis() - t2);   // todo
        System.out.println(readTime);
    }

    @Test
    public void readSpeedRTest10() throws IOException, InterruptedException {
        Random random = new Random();
        ArrayList<Long> list = new ArrayList<>();
        int num = 256;
        for (int i = 0; i < num; i ++ ) {
            list.add((long)random.nextInt((int)1e7));
        }
        Collections.sort(list);
        System.out.println(list);

        RandomAccessFile randomAccessFile = new RandomAccessFile(Parameters.FileSetting.inputPath + "10.bin", "r");//r: 只读模式 rw:读写模式

        Thread.sleep(3000);

        long readTime = 0;
        byte[] ts = new byte[Parameters.tsSize];
        long t2 = System.currentTimeMillis();   // todo
        for (int i = 0; i < num; i ++ ) {
//            System.out.println(list.get(i) * Parameters.tsSize);
            randomAccessFile.seek(list.get(i) * Parameters.tsSize);
//            Thread.sleep(100);
            int readSize = randomAccessFile.read(ts);
        }
        readTime += (System.currentTimeMillis() - t2);   // todo
        System.out.println(readTime);
    }
}
