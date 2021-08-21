package com.chriszt.flink.sql;


import org.junit.Test;

public class MyWorkTest {

    @Test
    public void testWork1() {
        String filePath = getClass().getResource("/tab1.csv").getPath();
        new MyWork().work1(filePath);
    }

}
