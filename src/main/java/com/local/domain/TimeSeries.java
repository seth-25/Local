package com.local.domain;

import com.local.util.CacheUtil;

public class TimeSeries {
    private byte[] timeSeriesData;
    private byte[] timeStamp;   // 小端存储的long

    public TimeSeries(byte[] timeSeriesData, byte[] timeStamp) {
        this.timeSeriesData = timeSeriesData;
        this.timeStamp = timeStamp;
    }

//    public TimeSeries(byte[] ts) {
//        byte[] timeSeriesData = new byte[Parameters.timeSeriesDataSize];
//        byte[] timeStamp = new byte[Parameters.timeStampSize];
//        System.arraycopy(ts, 0, timeSeriesData, 0,  Parameters.timeSeriesDataSize);
//        System.arraycopy(ts, Parameters.timeSeriesDataSize, timeStamp, 0, Parameters.timeStampSize);
//        this.timeSeriesData = timeSeriesData;
//        this.timeStamp = timeStamp;
//    }

    public byte[] getTimeSeriesData() {
        return timeSeriesData;
    }

    public byte[] getTimeStamp() {
        return timeStamp;
    }
}
