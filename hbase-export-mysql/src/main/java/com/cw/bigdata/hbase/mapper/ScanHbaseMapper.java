package com.cw.bigdata.hbase.mapper;

import com.cw.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ScanHbaseMapper extends TableMapper<Text, CacheData> {
    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

        CacheData data = new CacheData();
        for (Cell cell : result.rawCells()) {

            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            data.getDataMap().put(column, value);
        }
        context.write(new Text(Bytes.toString(key.get())), data);
    }
}
