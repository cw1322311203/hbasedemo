package com.cw.bigdata.hbase;


import com.cw.bigdata.util.HBaseUtil;

/**
 * 测试Hbase API
 */
public class TestHbaseAPI_3 {
    public static void main(String[] args) throws Exception {

        // 创建连接
        HBaseUtil.makeHbaseConnection();

        // 增加数据
        HBaseUtil.insertData("cw:student", "1002", "info", "name", "lisi");

        // 关闭连接
        HBaseUtil.close();
    }
}
