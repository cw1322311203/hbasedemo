package com.cw.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtils {
    public static Configuration conf;

    /**
     * TODO 获取Configuration对象
     */
    static {
        // 使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        // 如果在项目中没有加入hbase-site.xml配置文件,则可以采用以下方式配置
        //conf.set("hbase.zookeeper.quorum", "cm1.cdh.com,cm3.cdh.com,cm2.cdh.com");
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
    }


    /**
     * TODO 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        // 在HBase中管理、访问表需要先创建HBaseAdmin对象
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * TODO 创建表
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
        } else {
            // 创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            // 根据对表的配置,创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功!");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功!");
        } else {
            System.out.println("表" + tableName + "不存在!");
        }
    }

    /**
     * TODO 向表中插入数据
     *
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowkey, String columnFamily, String column, String value) throws IOException {
        // 创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        // 向表中插入数据
        Put put = new Put(Bytes.toBytes(rowkey));
        // 向put对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功!");
    }

    /**
     * TODO 删除多行数据
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    /**
     * TODO 获取所有数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        // 得到用于扫描region的对象
        Scan scan = new Scan();
        // 使用HTable得到resultscanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {

                // 得到rowkey
                System.out.println("******** 行键:" + Bytes.toString(CellUtil.cloneRow(cell)) + " *******");
                // 得到列族
                System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                // 列
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                // 值
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * TODO 获取某一行数据
     *
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowkey) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("******** 行键:" + Bytes.toString(result.getRow()) + " ********");
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    /**
     * TODO 获取某一行指定“列族:列”的数据
     *
     * @param tableName
     * @param rowkey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowkey, String family, String qualifier) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("******* 行键:" + Bytes.toString(result.getRow()) + " ******");
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }


    public static void createNameSpace(String namespace) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 1) 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(namespace + " 命名空间存在!");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        String tableName = "cw:student";
        if (isTableExist(tableName)) {
            getAllRows(tableName);
            getRow(tableName, "1001");
            getRowQualifier(tableName, "1002", "info", "name");
        }
    }
}
