package com.chriszt.flink.sql;


import org.junit.Test;

public class MyWorkTest {

    @Test
    public void testWork1() {
//        String filePath = getClass().getResource("/tab1.csv").getPath();
        String filePath = "/home/yl/proj/flink-demo/sql-cli/userTab.csv";
        new MyWork().work1(filePath);
    }

}