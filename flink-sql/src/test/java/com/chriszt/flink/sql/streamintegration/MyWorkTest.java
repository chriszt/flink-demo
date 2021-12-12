package com.chriszt.flink.sql.streamintegration;


import org.junit.Before;
import org.junit.Test;

public class MyWorkTest {

    private MyWork myWork;

    @Before
    public void initMyWork() {
        myWork = new MyWork();
    }

    @Test
    public void testWork1() {
//        String filePath = getClass().getResource("/tab1.csv").getPath();
        String filePath = "/home/yl/proj/flink-demo/sql-cli/userTab.csv";
        myWork.work1(filePath);
    }

    @Test
    public void testWork2() {
        myWork.work2();
    }

    @Test
    public void testWork3() {
        myWork.work3();
    }

    @Test
    public void testWork4() {
        // TODO
        myWork.work4();
    }

    @Test
    public void testWork5() {
        myWork.work5();
    }

    @Test
    public void testWork6() {
        myWork.work6();
    }

    @Test
    public void testWork7() {
        myWork.work7();
    }

    @Test
    public void testWork8() {
        myWork.work8();
    }

    @Test
    public void testWork9() {
        myWork.work9();
    }

    @Test
    public void testWork10() {
        myWork.work10();
    }

    @Test
    public void testWork11() {
        myWork.work11();
    }

}
