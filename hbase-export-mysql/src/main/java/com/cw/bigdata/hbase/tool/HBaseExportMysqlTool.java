package com.cw.bigdata.hbase.tool;

import com.cw.bigdata.hbase.bean.CacheData;
import com.cw.bigdata.hbase.format.MysqlOutputFormat;
import com.cw.bigdata.hbase.mapper.ScanHbaseMapper;
import com.cw.bigdata.hbase.reducer.Hbase2MysqlReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

import java.util.HashMap;
import java.util.Map;

/**
 * HBase导出到MySQL
 */
public class HBaseExportMysqlTool implements Tool {

    private Configuration configuration;

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HBaseExportMysqlTool.class);


        // 解析命令行参数
        // Map<String, String> paramMap = new HashMap<String, String>();
        for (int i = 0; i < args.length; i = i + 2) {

            String paramName = args[i];
            String paramValue = args[i + 1];

            if (!paramName.startsWith("--")) {
                throw new RuntimeException("参数传递不正确!");
            }

            if (paramValue.startsWith("--")) {
                throw new RuntimeException("参数传递不正确!");
            }

            configuration.set(paramName, paramValue);
        }

        Scan scan = new Scan();
        if (configuration.get("--hbase-family") != null) {
            scan.addFamily(Bytes.toBytes(configuration.get("--hbase-family")));
        }

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                configuration.get("--hbase-table"),
                scan,
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

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return null;
    }
}
