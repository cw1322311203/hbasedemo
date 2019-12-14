package com.cw.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase操作工具类
 */
public class HBaseUtil {

    // ThreadLocal
    // 只要在一个线程中缓存是共享的
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    //private static Connection conn = null;

    private HBaseUtil() {

    }

    /**
     * TODO 获取HBase连接对象
     *
     * @return
     * @throws Exception
     */
    public static void makeHbaseConnection() throws Exception {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            // 如果在项目中没有加入hbase-site.xml配置文件,则可以采用以下方式配置
            //conf.set("hbase.zookeeper.quorum", "cm1.cdh.com,cm3.cdh.com,cm2.cdh.com");
            //conf.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
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

    /**
     * TODO 增加数据
     *
     * @param rowkey
     * @param family
     * @param column
     * @param value
     * @throws Exception
     */
    public static void insertData(String tableName, String rowkey, String family, String column, String value) throws Exception {
        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
        table.close();
    }

    /**
     * TODO 关闭连接
     *
     * @throws Exception
     */
    public static void close() throws Exception {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }
}
