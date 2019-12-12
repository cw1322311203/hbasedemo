package com.cw.bigdata.hbase.coprocesser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 测试Hbase API
 */
public class TestHbaseAPI {
    public static void main(String[] args) throws Exception {

        // 通过java代码访问hbase数据库

        // 0) 创建配置对象,获取hbase的连接
        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);

        // 2) 获取操作对象 : Admin
        Admin admin = connection.getAdmin();

        // 3) 操作数据库

        // 3-1) 判断命名空间是否存在
        try {
            admin.getNamespaceDescriptor("cw");// 不存在会抛异常
        } catch (NamespaceNotFoundException e) {
            //创建表空间
            NamespaceDescriptor nd = NamespaceDescriptor.create("cw").build();
            admin.createNamespace(nd);
            System.out.println("命名空间创建成功...");
        }

        // 3-2) 判断hbase中是否存在某张表
        TableName tableName = TableName.valueOf("student");
        boolean flag = admin.tableExists(tableName);
        System.out.println(flag);

        if (flag) {

            // 获取指定的表对象
            Table table = connection.getTable(tableName);

            // 查询数据
            // Admin负责DDL(create drop alter) DML(update insert delete) DQL(select)
            String rowkey = "1001";
            // string==>byte[]
            // 字符编码问题
            Get get = new Get(Bytes.toBytes(rowkey));

            // 查询结果
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            System.out.println("1001数据是否存在= " + !empty);
            if (empty) {
                // 新增数据
                Put put = new Put(Bytes.toBytes(rowkey));

                String family = "info";
                String column = "name";
                String val = "zhangsan";

                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(val));

                table.put(put);
                System.out.println("增加数据...");
            } else {
                // 展示数据
                for (Cell cell : result.rawCells()) {
                    System.out.println("value = " + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("rowkey = " + Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("family = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("column = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
            }
        } else {
            //创建表

            // 创建表描述对象
            HTableDescriptor td = new HTableDescriptor(tableName);

            // 增加协处理器
            td.addCoprocessor("com.cw.bigdata.hbase.coprocesser.InsertCwStudentCoprocesser");

            // 增加列族
            HColumnDescriptor cd = new HColumnDescriptor("info");
            td.addFamily(cd);

            admin.createTable(td);

            System.out.println(tableName + "表创建成功...");
        }

        // 4) 获取操作结果

        // 5) 关闭数据库连接

    }
}
