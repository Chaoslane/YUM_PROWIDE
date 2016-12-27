package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 自定义 key
 * field1为 用户id
 * field2为用户行为时间戳
 */
public class DefinedKey implements WritableComparable<DefinedKey> {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);
    private String deviceId;
    private String timeStr;

    public DefinedKey() {
        super();
    }

    public DefinedKey(String deviceId, String timeStr) {
        super();
        this.deviceId = deviceId;
        this.timeStr = timeStr;
    }

    @Override
    public int compareTo(DefinedKey o) {
        return this.deviceId.compareTo(o.getDeviceId());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        deviceId = in.readUTF();
        timeStr = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!out.equals(null)) {
            out.writeUTF(deviceId);
            out.writeUTF(timeStr);
        }
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(String timeStr) {
        this.timeStr = timeStr;
    }

    @Override
    public String toString() {
        return deviceId+"\t"+timeStr;
    }
}
