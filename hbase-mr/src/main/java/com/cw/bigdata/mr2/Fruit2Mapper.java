package com.cw.bigdata.mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 构建Put对象
        Put put = new Put(key.get());

        // 1.获取数据
        for (Cell cell : value.rawCells()) {

            // 2.判断当前的cell是否为"name"列
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                // 3.给Put对象赋值
                put.add(cell);
            }
        }

        // 4.写出
        context.write(key, put);
    }
}
