package leveldb_sax;


import com.local.domain.Parameters;
import com.local.search.SearchAction;
import com.local.version.VersionAction;

public class db_send {

    //javap -verbose db_send.class
    public static void send_edit(byte[] edit) {
        //第一个字节为0， 发送versionid 4字节, amV_id 4字节, number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节，

        //第一个字节为1，发送versionid 4字节， 删除的个数n1 4字节，(number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节) * n1
        //增加的个数n2 4字节，(number 8字节，saxt_smallest 8字节，saxt_biggest 8字节，startTime 8字节，endTime 8字节) * n2
        VersionAction.changeVersion(edit, Parameters.hostName);
        VersionAction.checkWorkerVersion();
    }

    // 发送 info
    // info(有时间戳): ts 256*4, starttime 8, endtime 8, k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4, p * n 8*n
    // info(没时间戳): ts 256*4, k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    // 返回至多k个ares(含p)
    // ares(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐), p 8
    // ares(没时间戳): ts 256*4, float dist 4, 空4位(p是long,对齐), p 8
    public static byte[] find_tskey(byte[] info) {
//        return SearchAction.searchOriTs(info, false);
        return null;
    }

    // 发送 info
    // info(有时间戳): ts 256*4，starttime 8， endtime 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    // info(没时间戳): ts 256*4， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    // 返回至若干个ares_exact(不含p)
    // ares_exact(有时间戳): ts 256*4, time 8, float dist 4, 空4位(time是long,对齐)
    // ares_exact(没时间戳): ts 256*4, float dist 4
    public static byte[] find_tskey_exact(byte[] info) {
//        return SearchAction.searchOriTs(info, true);
        return null;
    }


    // 发送 info
    // info(有时间戳): ts 256*4，starttime 8， endtime 8, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    // info(没时间戳): ts 256*4, heap 8， k 4, 还要多少个needNum 4, topdist 4, 要查的个数n 4，p * n 8*n
    public static void find_tskey_ap(byte[] info) {
        SearchAction.searchOriTsPushHeap(info, false);
    }

    public static void find_tskey_exact_ap(byte[] info) {
        SearchAction.searchOriTsPushHeap(info, true);
    }
}
