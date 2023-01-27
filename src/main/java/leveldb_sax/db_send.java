package leveldb_sax;


import com.local.domain.Parameters;
import com.local.search.SearchAction;
import com.local.version.VersionAction;

import java.util.Arrays;

public class db_send {

    //javap -verbose db_send.class
    public static void send_edit(byte[] edit) {
        //第一个字节为0， 发送versionid 4字节, amV_id 4字节, number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节，

        //第一个字节为1，发送versionid 4字节， 删除的个数n1 4字节，(number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节) * n1
        //增加的个数n2 4字节，(number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节) * n2

//        int versionid = (edit[4]&0xff)<<24|(edit[3]&0xff)<<16|(edit[2]&0xff)<<8|(edit[1]&0xff);
//        int amV_id = (edit[8]&0xff)<<24|(edit[7]&0xff)<<16|(edit[6]&0xff)<<8|(edit[5]&0xff);
//        System.out.println("send_edit:"+edit[0]+" "+versionid + " " + amV_id);
//        System.out.println("edit长度" + edit.length + " " + Arrays.toString(edit));

        System.out.println("开始更新版本");
        VersionAction.changeVersion(edit, Parameters.hostName);
        VersionAction.checkWorkerVersion();
    }

    //返回至多k个
    public static byte[] find_tskey(byte[] info) {

        // 返回至多k个ares 1040一个 见db
        return SearchAction.searchNearlyTs(info);
    }


}
