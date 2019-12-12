package com.cw.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL:(Admin对象)
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * <p>
 * DML:(Table对象)
 * 5.插入数据
 * 6.查数据(get)
 * 7.查数据(scan)
 * 8.删除数据
 */
public class TestAPI {


    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            // 1.获取配置文件信息
            // HBaseConfiguration configuration = new HBaseConfiguration();// 已过时
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "cm1.cdh.com,cm3.cdh.com,cm2.cdh.com");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");// 可写可不写,默认为2181(hbase-default.xml文件可查看)

            // 2.创建连接对象
            connection = ConnectionFactory.createConnection(configuration);

            // 3.创建Admin对象
            //HBaseAdmin admin = new HBaseAdmin(configuration); // 已过时
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        // 1.判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        // 2.返回结果
        return exists;
    }


    /**
     * TODO 创建表
     *
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    public static void createTable(String tableName, String... cfs) throws IOException {

        // 1.判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息!");
            return;
        }

        // 2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在!");
            return;
        }

        // 3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 4.循环添加列族信息
        for (String cf : cfs) {
            // 5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            // 6.添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 7.创建表
        admin.createTable(hTableDescriptor);

    }

    /**
     * TODO 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {

        // 1.判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在!");
            return;
        }

        // 2.使表下线
        admin.disableTable(TableName.valueOf(tableName));

        // 3.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * TODO 创建命名空间
     *
     * @param namespace
     */
    public static void createNameSpace(String namespace) {

        // 1.创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

        // 2.创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(namespace + "命名空间已存在!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO 向表中插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        // 1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 3.给Put对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        // 4.插入数据
        table.put(put);

        // 5.关闭表连接
        table.close();
    }

    /**
     * TODO 获取数据（get）
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 2.1 指定获取的列族
        //get.addFamily(Bytes.toBytes(columnFamily));

        // 2.2 指定列族和列
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        // 2.3 设置获取数据的版本数
        get.setMaxVersions(5);

        // 3.获取数据
        Result result = table.get(get);

        // 4.解析result并打印
        for (Cell cell : result.rawCells()) {

            // 5.打印数据
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    "，CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));

        }

        // 6.关闭表连接
        table.close();
    }

    /**
     * TODO 扫描全表数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void scanTable(String tableName) throws IOException {

        // 1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.构建Scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));// 左闭右开

        // 3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        // 4.解析resultScanner
        for (Result result : resultScanner) {

            // 5.解析result并打印
            for (Cell cell : result.rawCells()) {

                // 6.打印数据
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        "，CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "，CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        // 7.关闭表连接
        table.close();
    }


    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        // 1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));


        /*

           Delete标记: 删除特定列列指定的版本
           DeleteFamily标记: 删除特定列族所有列
           DeleteColumn标记: 删除特定列的所有版本


           指定rowkey: 使用DeleteFamily标记
           ---->不加时间戳表示删除[指定rowkey]的所有数据,加时间戳表示删除[指定rowkey]中[时间戳版本小于或等于指定时间戳]的所有数据
           指定rowkey+columnFamily: 使用DeleteFamily标记
           ---->不加时间戳表示删除[指定列族]的所有数据,加了时间戳就表示删除[指定列族]下[时间戳版本小于或等于指定时间戳]的所有数据
           指定rowkey+columnFamily+column(addColumns): 使用DeleteColumn标记
           ---->不加时间戳表示删除[指定列]所有版本的数据,加时间戳表示删除[指定列]中[时间戳版本小于或等于指定时间戳]的所有数据。
           指定rowkey+columnFamily+column(addColumn): 使用Delete标记 (只删除单个版本数据,生产环境尽量别用)
           ---->不加时间戳表示删除[指定列]中[最新版本]的数据,加时间戳表示删除[指定列]中[指定时间戳版本]的数据。
           ---->不推荐的原因是:操作不同(如flush前后操作产生的结果会不一样)结果可能不同
                 如:在flush前如果有多个版本的数据,此时进行addColumn(不加时间戳)操作,会将最新版本的数据删除,然后老版本的数据会出现
                 在flush后进行addColumn(不加时间戳)操作,会将最新版本的数据删除,而此时flush已将老版本的数据进行了删除,所有此时老版本的数据就不会出现了

         */

        // 2.1 设置删除的列
        // 删除列最好使用addColumns
        // addColumns:不加时间戳表示删除指定列所有版本的数据(推荐)
        // addColumns:加时间戳表示删除时间戳小于或等于指定时间戳的指定列的所有版本。
        // addColumn:不加时间戳表示删除最新版本的数据,操作不同(如flush前后操作产生的结果会不一样)结果可能不同
        // addColumn:加时间戳表示删除指定时间戳的指定列版本的数据。

        //delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn),1574158036021L);

        // 2.2 删除指定的列族
        // addFamily:删除指定列族的所有列的所有版本数据。
        delete.addFamily(Bytes.toBytes(cf));

        // 3.执行删除操作
        table.delete(delete);

        // 4.关闭连接
        table.close();
    }

    /**
     * TODO 关闭资源
     */
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        // 1.测试表是否存在
        //System.out.println(isTableExist("student1"));

        // 2.创建表测试
        //createTable("wc:student1", "info1", "info2");

        // 3.删除表测试
        //dropTable("student1");
        //System.out.println(isTableExist("student1"));

        // 4.创建命名空间测试
        //createNameSpace("wc");

        // 5.插入数据测试
        //putData("student", "1003", "info", "name", "John");

        // 6.获取单行数据测试
        //getData("student", "1001", "info", "name");

        // 7.测试扫描数据
        scanTable("student");

        // 8.删除测试
        //deleteData("student", "1005", "info", "name");

        // 关闭资源
        close();
    }
}
