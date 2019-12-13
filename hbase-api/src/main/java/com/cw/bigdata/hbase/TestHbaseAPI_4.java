package com.cw.bigdata.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 查询数据(使用过滤器Filter)
 */
public class TestHbaseAPI_4 {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("cw:student");

        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes("info"));
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("2001"));
        RegexStringComparator rsc = new RegexStringComparator("^\\d{3}$");
        //Filter f = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, bc);// 大于等于2001
        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL, rsc);// rowkey中包含的数字为3

        // FilterList.Operator.MUST_PASS_ALL : and
        // FilterList.Operator.MUST_PASS_ONE : or
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE); // or

        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, bc);

        list.addFilter(f);
        list.addFilter(rf);

        // 扫描时增加过滤器
        // TODO 所谓的过滤,其实每条数据都会筛选过滤,性能比较低
        scan.setFilter(list);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {

                System.out.println("value = " + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey = " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("------------------------");
            }
        }

        table.close();
        connection.close();
    }
}
