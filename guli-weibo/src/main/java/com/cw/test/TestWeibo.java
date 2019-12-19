package com.cw.test;

import com.cw.constants.Constants;
import com.cw.dao.HBaseDao;
import com.cw.utils.HBaseUtil;

import java.io.IOException;

public class TestWeibo {

    public static void init() {

        try {
            // 创建命名空间
            System.out.println("---------- 开始创建命名空间 ----------");
            HBaseUtil.createNameSpace(Constants.NAMESPACE);

            // 创建微博内容表
            System.out.println("--------- 开始创建微博内容表 -------------");
            HBaseUtil.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONS, Constants.CONTENT_TABLE_CF);

            // 创建用户关系表
            System.out.println("--------- 开始创建用户关系表 -------------");
            HBaseUtil.createTable(Constants.RELATION_TABLE, Constants.RELATION_TABLE_VERSIONS, Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);

            // 创建收件箱表
            System.out.println("--------- 开始创建收件箱表 -------------");
            HBaseUtil.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSIONS, Constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        // 初始化
        init();

        // 1001发布微博
        HBaseDao.publishWeiBo("1001", "今天天气不错!");

        // 1002关注1001和1003
        HBaseDao.addAttends("1002", "1001", "1003");

        // 获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********111**********");

        // 1003发布3条微博,同时1001发布两条微博
        HBaseDao.publishWeiBo("1003", "今晚月亮挺圆!");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001", "今晚天气不错!");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1003", "天气不好!!!");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001", "今晚天气真的不错!");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1003", "天气真的不好!!!!");


        // 获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********222**********");

        // 1002取关1003
        HBaseDao.deleteAttends("1002", "1003");

        // 获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********333**********");

        // 1002再次关注1003
        HBaseDao.addAttends("1002", "1003");

        // 获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("********444**********");

        // 获取1001微博详情
        HBaseDao.getWeiBo("1001");

    }
}
