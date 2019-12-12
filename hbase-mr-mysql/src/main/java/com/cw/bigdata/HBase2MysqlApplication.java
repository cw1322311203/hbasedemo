package com.cw.bigdata;

import com.cw.bigdata.tool.HBase2MysqlTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 对HBase中的student表数据的value值进行wordcount,并写入MySQL
 */
public class HBase2MysqlApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBase2MysqlTool(), args);
    }
}
