package com.cw.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 测试Hbase API
 */
public class TestHbaseAPI_5 {
    public static void main(String[] args) throws Exception {


        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);

        // TODO 创建预分区表

        /*
        Admin admin = connection.getAdmin();

        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("emp1"));

        HColumnDescriptor cd = new HColumnDescriptor("info");
        td.addFamily(cd);

        byte[][] bs = new byte[2][];
        bs[0] = Bytes.toBytes("0|");
        bs[1] = Bytes.toBytes("1|");
        // 可以写为

        byte[][] bs = genRegionKeys(3);

        // [byte[],byte[]]
        // 创建表的同时,增加预分区
        admin.createTable(td, bs);

        System.out.println("表格创建成功...");

        */

        // TODO 增加数据
        Table empTable = connection.getTable(TableName.valueOf("emp1"));

        String rowkey = "lisi";


        // hashmap
        // 将rowkey均匀的分配到不同的分区中,效果和hashmap数据存储的规则是一样的

        // HashMap

        rowkey = genRegionNum(rowkey, 3);
        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("23"));

        empTable.put(put);

    }


    /**
     * 生成分区键
     *
     * @param regionCount
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount - 1][];

        for (int i = 0; i < regionCount - 1; i++) {
            bs[i] = Bytes.toBytes(i + "|");
        }
        return bs;
    }

    /**
     * 生成分区号
     *
     * @param rowkey
     * @param regionCount
     * @return
     */
    public static String genRegionNum(String rowkey, int regionCount) {

        int regionNum;
        int hash = rowkey.hashCode();

        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            // 2^n
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % regionCount;
        }

        return regionNum + "_" + rowkey;
    }
}
