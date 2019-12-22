package com.cw.bigdata.hbase;

import com.cw.bigdata.hbase.tool.HBaseExportMysqlTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 封装后的代码
 * 可使用以下命令导出
 *      yarn jar xxx.jar --hbase-table emp --hbase-family info  --mysql-table student
 */
public class HBaseExportMysqlApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseExportMysqlTool(),args);
    }
}
