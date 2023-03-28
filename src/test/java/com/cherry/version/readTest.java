package com.cherry.version;

import com.cherry.domain.Parameters;
import com.cherry.util.MappedFileReader;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class readTest {

    @Test
    public void readSpeedBatchTest100() throws IOException {
        Random random = new Random();
        ArrayList<Integer> list = new ArrayList<>();
        int num = 100000;

        for (int i = 0; i < num; i ++ ) {
            list.add(random.nextInt((int)1e8));
        }


        ArrayList<ArrayList<Integer>> list1 = new ArrayList<>();
        ArrayList<Integer> list2 = new ArrayList<>();
        for (int i = 1; i <= num; i ++ ) {
            list2.add(list.get(i - 1));
            if (i % 100 == 0) {
                Collections.sort(list2);
                list1.add(list2);
                list2 = new ArrayList<>();
            }
        }
        Collections.sort(list);

        System.out.println(list);

        File file = new File(Parameters.FileSetting.inputPath + "100.bin");
        MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, 1);  // 初始化的ts

        long readTime = 0;
        byte[] ts;
        for (int i = 0; i < num; i ++ ) {
            int index = list.get(i);
            long t2 = System.currentTimeMillis();   // todo
            ts = reader.readTs(index);
            readTime += (System.currentTimeMillis() - t2);   // todo
        }
        System.out.println(readTime);

        long readTime1 = 0;
        byte[] ts1;
        for (ArrayList<Integer> integers : list1) {
            for (int i = 0; i < num; i ++ ) {
                int integer = integers.get(i);
                long t2 = System.currentTimeMillis();   // todo
                ts1 = reader.readTs(integer);
                readTime1 += (System.currentTimeMillis() - t2);   // todo
            }
        }
        System.out.println(readTime1);
    }
    @Test
    public void readSpeedTest100() throws IOException {
        Random random = new Random();
        ArrayList<Integer> list = new ArrayList<>();
        int num = 256;

        for (int i = 0; i < num; i ++ ) {
            list.add(random.nextInt((int)1e8));
//            list.add(i);
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

    @Test
    public void readFileTest() throws IOException {
        int size = 1024;

        ArrayList<byte[]> dataSet = new ArrayList<>();
        FileInputStream input = new FileInputStream("./1.bin");
        while(true){
            byte[] bytes = new byte[size];
            if (input.read(bytes) == -1) break;
            dataSet.add(bytes);
        }

        ArrayList<byte[]> query = new ArrayList<>();
        input = new FileInputStream("./01.bin");
        while(true){
            byte[] bytes = new byte[size];
            if (input.read(bytes) == -1) break;
            query.add(bytes);
        }

        System.out.println(Arrays.toString(dataSet.get(1)) + " " + dataSet.size());
        System.out.println(Arrays.toString(query.get(1)) + " " + query.size());

        for (int i = 0; i < query.size(); i ++ ) {
            byte[] queryBytes = query.get(i);
            for (int j = 0; j < dataSet.size(); j ++ ) {
                byte[] dataBytes = dataSet.get(j);
                boolean flag = false;
                for (int k = 0; k < size; k ++ ) {
                    if (queryBytes[k] != dataBytes[k]) {
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    System.out.println("相等 " + i  + " " + j);
                }
            }
        }
    }
}
