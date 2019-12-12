package com.cw.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 测试Hbase API
 */
public class TestHbaseAPI_2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("cw:student");
        // TODO 删除表
        /*
        TableName tableName = TableName.valueOf("cw:student");

        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {

            // 禁用表
            admin.disableTable(tableName);

            // 删除表
            admin.deleteTable(tableName);
        }
        */

        // TODO 删除数据
        /*
        Table table = connection.getTable(tableName);

        // delete
        String rowkey = "1001";
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        System.out.println("删除数据...");
        */

        // TODO 扫描数据
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value = " + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey = " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }

    }
}
