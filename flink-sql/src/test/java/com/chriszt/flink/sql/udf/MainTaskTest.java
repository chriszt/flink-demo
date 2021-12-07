package com.chriszt.flink.sql.udf;

import org.junit.Before;
import org.junit.Test;

public class MainTaskTest {

    private MainTask mainTask = null;

    @Before
    public void init() {
        mainTask = new MainTask();
    }

    @Test
    public void testTask1() {
        mainTask.task1();
    }

    @Test
    public void testTask2() {
        mainTask.task2();
    }

    @Test
    public void testTask3() {
        mainTask.task3();
    }

}
