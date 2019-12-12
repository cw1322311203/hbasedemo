package com.cw.bigdata.reducer;

import com.cw.bigdata.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Hbase2MysqlReducer extends Reducer<Text, CacheData, Text, CacheData> {
    @Override
    protected void reduce(Text key, Iterable<CacheData> datas, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (CacheData data : datas) {
            sum += data.getCount();
        }

        CacheData sumData = new CacheData();
        sumData.setName(key.toString());
        sumData.setCount(sum);

        System.err.println(sumData.getName() + ":" + sumData.getCount());

        context.write(key, sumData);
    }
}
