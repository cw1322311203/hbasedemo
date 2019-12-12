package com.cw.bigdata.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 目标：将HBase中fruit1表中的一部分数据(name列)，通过MR迁入到HBase的fruit2表中。
 */
public class Fruit2Driver implements Tool {

    // 定义配置信息
    private Configuration configuration = null;

    public int run(String[] args) throws Exception {

        // 1.获取Job对象
        Job job = Job.getInstance(configuration);

        // 2.设置主类路径
        job.setJarByClass(Fruit2Driver.class);

        // 3.设置Mapper和输出KV类型
        TableMapReduceUtil.initTableMapperJob("fruit1",
                new Scan(),
                Fruit2Mapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        // 4.设置Reducer&输出的表
        TableMapReduceUtil.initTableReducerJob("fruit2",
                Fruit2Reducer.class,
                job);

        // 5.提交任务
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration, new Fruit2Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
