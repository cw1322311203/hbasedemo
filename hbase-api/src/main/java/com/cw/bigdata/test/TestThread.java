package com.cw.bigdata.test;

public class TestThread {
    public static void main(String[] args) throws InterruptedException {

        // 多例
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            s.append(i);
        }
        System.out.println(s);
    }
}
