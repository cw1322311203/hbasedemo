package com.cw.bigdata.hbase.reducer;

import com.cw.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Hbase2MysqlReducer extends Reducer<Text, CacheData, Text, CacheData> {
    @Override
    protected void reduce(Text key, Iterable<CacheData> datas, Context context) throws IOException, InterruptedException {

        for (CacheData data : datas) {
            context.write(key, data);
        }
    }
}
