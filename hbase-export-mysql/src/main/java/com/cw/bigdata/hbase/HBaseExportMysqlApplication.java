package com.cw.bigdata.hbase;

import com.cw.bigdata.hbase.tool.HBaseExportMysqlTool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseExportMysqlApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseExportMysqlTool(),args);
    }
}
