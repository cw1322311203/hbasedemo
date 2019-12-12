package com.cw.bigdata.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CacheData implements WritableComparable<CacheData> {

    private String name;
    private int count;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int compareTo(CacheData data) {
        return name.compareTo(data.name);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        count = in.readInt();
    }
}
