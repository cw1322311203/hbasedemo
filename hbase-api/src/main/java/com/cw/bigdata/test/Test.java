package com.cw.bigdata.test;

import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;

import java.lang.management.MemoryUsage;

public class Test {
    public static void main(String[] args) {
        final MemoryUsage usage = HeapMemorySizeUtil.safeGetHeapMemoryUsage();

        // -Xms  1/64  32G*1/64=0.5G
        System.out.println(usage.getInit());// 536870912 0.5G
        // -Xmx  1/4   32G*1/4=8G
        System.out.println(usage.getMax());// 7616856064 7.09375G
    }
}
