package com.cherry.version;

import com.cherry.Parameters;
import com.cherry.util.DBUtil;
import com.cherry.util.FileUtil;
import com.cherry.util.MappedFileReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class saxtTest {
    public static void main(String[] args) throws IOException {
        FileUtil.deleteFolderFiles("./db");
        DBUtil.dataBase.open("db");

        File file = new File("./ts/10.bin");
        MappedFileReader reader = new MappedFileReader(file.getPath(), Parameters.FileSetting.readSize, 0);
        byte[] searchTsBytes = reader.readTs(1);

        System.out.println(Arrays.toString(searchTsBytes));
        byte[] saxTData = new byte[Parameters.saxTSize];
        float[] paa = new float[Parameters.paaNum];
        DBUtil.dataBase.paa_saxt_from_ts(searchTsBytes, saxTData, paa);
        System.out.println("saxT: "  + Arrays.toString(saxTData));
        System.out.println("paa: "  + Arrays.toString(paa));
    }
}
