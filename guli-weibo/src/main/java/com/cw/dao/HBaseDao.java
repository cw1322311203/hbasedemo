package com.cw.dao;

import com.cw.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1.发布微博
 * 2.删除微博
 * 3.关注用户
 * 4.取关用户
 * 5.获取用户的初始化页面
 * 6.获取用户微博详情
 */
public class HBaseDao {

    /**
     * TODO 发布微博
     *
     * @param uid
     * @param content
     * @throws IOException
     */
    public static void publishWeiBo(String uid, String content) throws IOException {

        // 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // TODO 第一部分:操作微博内容表
        // 1.获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        // 2.获取当前时间戳
        long ts = System.currentTimeMillis();

        // 3.获取RowKey
        String rowKey = uid + "_" + ts;

        // 4.创建Put对象
        Put contentPut = new Put(Bytes.toBytes(rowKey));

        // 5.给Put对象赋值
        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        // 6.执行插入数据操作
        contentTable.put(contentPut);

        // TODO 第二部分:操作微博收件箱表
        // 1.获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        // 2.获取当前发布微博人的fans列族数据
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get);

        // 3.创建一个集合,用于存放微博内容表的Put对象
        ArrayList<Put> inboxPuts = new ArrayList<Put>();

        // 4.遍历粉丝
        for (Cell cell : result.rawCells()) {

            // 5.构建微博收件箱表的Put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));

            // 6.给收件箱表的Put对象赋值
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));

            // 7.将收件箱表的Put对象存入集合
            inboxPuts.add(inboxPut);
        }

        // 8.判断是否有粉丝
        if (inboxPuts.size() > 0) {

            // 获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            // 执行收件箱表数据插入操作
            inboxTable.put(inboxPuts);

            // 关闭收件箱表
            inboxTable.close();
        }

        // 关闭资源
        relationTable.close();
        contentTable.close();
        connection.close();

    }

    /**
     * TODO 关注用户
     *
     * @param uid
     * @param attends
     */
    public static void addAttends(String uid, String... attends) throws IOException {

        // 校验是否添加了待关注的人
        if (attends.length <= 0) {
            System.out.println("请选择待关注的人！");
            return;
        }

        // 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // TODO 第一部分：操作用户关系表
        // 1.获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        // 2.创建一个集合，用于存放用户关系表Put对象
        ArrayList<Put> relationPuts = new ArrayList<Put>();

        // 3.创建操作者的Put对象
        Put uidPut = new Put(Bytes.toBytes(uid));

        // 4.循环创建被关注者的Put对象
        for (String attend : attends) {

            // 5.给操作者的Put对象赋值
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));

            // 6.创建被关注者的Put对象
            Put attendPut = new Put(Bytes.toBytes(attend));

            // 7.给被关注者的Put对象赋值
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));

            // 8.将被关注者的Put对象放入集合
            relationPuts.add(attendPut);
        }

        // 9.将操作者的Put对象添加到集合
        relationPuts.add(uidPut);

        // 10.执行用户关系表的插入数据操作
        relationTable.put(relationPuts);


        // TODO 第二部分：操作收件箱表

        // 1.获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        // 2.创建收件箱表的Put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        // 3.循环attends,获取每个被关注者的近期发布的微博
        for (String attend : attends) {

            // 4.获取当前被关注者的近期发布的微博(scan) -> 集合ResultScanner
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contentTable.getScanner(scan);

            // 定义一个时间戳
            long ts = System.currentTimeMillis();

            // 5.对获取的值进行遍历
            for (Result result : resultScanner) {

                // 6.给收件箱表的Put对象赋值
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());

            }


        }

        // 7.判断当前的Put对象是否为空
        if (!inboxPut.isEmpty()) {

            // 获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            // 插入数据
            inboxTable.put(inboxPut);

            // 关闭收件箱表连接
            inboxTable.close();

        }

        // 关闭资源
        relationTable.close();
        contentTable.close();
        connection.close();

    }

    /**
     * TODO 取关用户
     *
     * @param uid
     * @param dels
     */
    public static void deleteAttends(String uid, String... dels) throws IOException {

        if (dels.length <= 0) {
            System.out.println("请添加待取关的用户!");
            return;
        }

        // 获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 第一部分:操作用户关系表
        // 1.获取用户关系表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        // 2.创建一个集合,用于存放用户关系表的Delete对象
        ArrayList<Delete> relationDeletes = new ArrayList<Delete>();

        // 3.创建操作者的Delete对象
        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        // 4.循环创建被取关者的Delete对象
        for (String del : dels) {

            // 5.给操作者的Delete对象赋值
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(del));

            // 6.创建被取关者的Delete对象
            Delete delDelete = new Delete(Bytes.toBytes(del));

            // 7.给被取关者的Delete对象赋值
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid));

            // 8.将被取关者的Delete对象添加至集合
            relationDeletes.add(delDelete);

        }

        // 9.将操作者的Delete对象添加至集合
        relationDeletes.add(uidDelete);

        // 10.执行用户关系表的删除操作
        relationTable.delete(relationDeletes);

        // 第二部分:操作收件箱表
        // 1.获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        // 2.创建操作者的Delete对象
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        // 3.给操作者的Delete对象赋值
        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(del));
        }

        // 4.执行收件箱表的删除操作
        inboxTable.delete(inboxDelete);

        // 关闭资源
        relationTable.close();
        inboxTable.close();
        connection.close();

    }

    /**
     * TODO 获取某个人的初始化页面数据
     *
     * @param uid
     */
    public static void getInit(String uid) throws IOException {

        // 1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 2.获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        // 3.获取微博内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        // 4.创建收件箱表Get对象,并获取数据(设置最大版本)
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);

        // 5.遍历获取的数据
        for (Cell cell : result.rawCells()) {

            // 6.构建微博内容表Get对象
            Get contentGet = new Get(CellUtil.cloneValue(cell));

            // 7.获取该Get对象的数据内容
            Result contentResult = contentTable.get(contentGet);

            // 8.解析内容并打印
            for (Cell contentCell : contentResult.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contentCell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(contentCell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(contentCell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(contentCell)));
            }

        }

        // 9.关闭资源
        inboxTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * TODO 获取某个人的所有微博详情
     *
     * @param uid
     */
    public static void getWeiBo(String uid) throws IOException {

        // 1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 2.获取微博内容表对象
        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        // 3.构建Scan对象
        Scan scan = new Scan();

        // 构建过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));

        scan.setFilter(rowFilter);

        // 4.获取数据
        ResultScanner resultScanner = table.getScanner(scan);

        // 5.解析数据并打印
        for (Result result : resultScanner) {

            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }


        // 6.关闭资源
        connection.close();
        table.close();

    }

}
