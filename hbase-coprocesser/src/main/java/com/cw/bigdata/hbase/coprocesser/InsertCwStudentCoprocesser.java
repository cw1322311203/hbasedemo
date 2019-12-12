package com.cw.bigdata.hbase.coprocesser;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 协处理器(Hbase自己的功能)实现两表的同步数据插入
 * 1)创建类,继承BaseRegionObserver
 * 2)重写方法:postPut
 * 3)实现逻辑:
 * 1. 增加student的数据
 * 2. 同时增加cw:student中的数据
 * 4)将项目打包(依赖)后上传到HBase安装目录的lib目录中(集群所有节点都需要上传),让hbase可以识别协处理器
 * 5)删除原始表,在增加新表时,同时设定协处理器td.addCoprocessor("com.cw.bigdata.hbase.coprocesser.InsertCwStudentCoprocesser");
 */
public class InsertCwStudentCoprocesser extends BaseRegionObserver {

    // prePut:之前
    // doPut:正在执行
    // postPut:之后,执行完插入操作之后执行什么操作
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("cw:student"));

        // 增加数据
        table.put(put);

        // 关闭表
        table.close();
    }
}
