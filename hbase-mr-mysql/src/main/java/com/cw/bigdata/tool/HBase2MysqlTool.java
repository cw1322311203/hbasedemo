package com.cw.bigdata.tool;

import com.cw.bigdata.bean.CacheData;
import com.cw.bigdata.format.MysqlOutputFormat;
import com.cw.bigdata.mapper.ScanHbaseMapper;
import com.cw.bigdata.reducer.Hbase2MysqlReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HBase2MysqlTool implements Tool {
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(HBase2MysqlTool.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                "student",
                new Scan(),
                ScanHbaseMapper.class,
                Text.class,
                CacheData.class,
                job
        );


        // reducer
        job.setReducerClass(Hbase2MysqlReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CacheData.class);

        job.setOutputFormatClass(MysqlOutputFormat.class);

        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
