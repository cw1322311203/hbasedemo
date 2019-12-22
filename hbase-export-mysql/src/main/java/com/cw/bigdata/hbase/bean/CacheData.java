package com.cw.bigdata.hbase.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CacheData implements WritableComparable<CacheData> {

    private String rowkey;
    private Map<String, String> dataMap = new HashMap<String, String>();

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public Map<String, String> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, String> dataMap) {
        this.dataMap = dataMap;
    }

    public int compareTo(CacheData data) {
        return rowkey.compareTo(data.rowkey);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(rowkey);

        // map ==> string = write
        // name=zhangsan,age=20,sex=0

        StringBuilder sb = new StringBuilder();

        Iterator<String> iterator = dataMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            String val = dataMap.get(key);
            sb.append(key).append("=").append(val).append(",");
        }

        out.writeUTF(sb.toString());
    }

    public void readFields(DataInput in) throws IOException {
        rowkey = in.readUTF();

        // read ==> string ==> map
        String dataString = in.readUTF();
        String[] datas = dataString.split(",");
        for (String data : datas) {
            String[] split = data.split("=");
            dataMap.put(split[0], split[1]);
        }
    }
}
