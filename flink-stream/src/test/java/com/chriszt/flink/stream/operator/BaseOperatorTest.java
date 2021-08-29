package com.chriszt.flink.stream.operator;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BaseOperatorTest {

    @Test
    public void testReadTextFile() throws Exception {
        String filePath = getClass().getResource("/BaseTextInput.txt").getPath();
        new BaseOperator().readTextFile(filePath);
    }

    @Test
    public void testReadFile() throws Exception {
        String filePath = getClass().getResource("/BaseTextInput.txt").getPath();
        new BaseOperator().readFile(filePath);
    }

    @Test
    public void testWriteToScreen() throws Exception {
        new BaseOperator().writeToScreen();
    }

    @Test
    public void testMapTask() throws Exception {
        new BaseOperator().mapTask();
    }

    @Test
    public void testFilterTemplate() throws Exception {
        new BaseOperator().filterTemplate();
    }

    @Test
    public void testKeyByTask() {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("123xxxx", 899, "2018-06"));
        lst.add(new Trade("123xxxx", 699, "2018-06"));
        lst.add(new Trade("188xxxx", 88, "2018-07"));
        lst.add(new Trade("188xxxx", 69, "2018-07"));
        lst.add(new Trade("158xxxx", 100, "2018-06"));
        lst.add(new Trade("158xxxx", 1000, "2018-06"));
        new BaseOperator().keyByTask(lst);
    }

    @Test
    public void testReduceTask() throws Exception {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("123xxxx", 899, "2018-06"));
        lst.add(new Trade("123xxxx", 699, "2018-06"));
        lst.add(new Trade("123xxxx", 88, "2018-07"));
        lst.add(new Trade("123xxxx", 69, "2018-07"));
        lst.add(new Trade("123xxxx", 100, "2018-06"));
        lst.add(new Trade("188xxxx", 1000, "2018-06"));
        new BaseOperator().reduceTask(lst);
    }

    @Test
    public void testAggTask() throws Exception {
        List<Trade> lst = new ArrayList<>();
        lst.add(new Trade("188xxxx", 30, "2018-07"));
        lst.add(new Trade("188xxxx", 20, "2018-11"));
        lst.add(new Trade("158xxxx", 1, "2018-07"));
        lst.add(new Trade("158xxxx", 2, "2018-06"));
        new BaseOperator().aggTask(lst);
    }

    @Test
    public void testSideOutputTemplate() throws Exception {
        new BaseOperator().sideOutputTemplate();
    }

    @Test
    public void testProjectTemplate() throws Exception {
        new BaseOperator().projectTemplate();
    }

    @Test
    public void testUnionTemplate() throws Exception {
        new BaseOperator().unionTemplate();
    }

    @Test
    public void testCoMapTemplate() throws Exception {
        new BaseOperator().coMapTemplate();
    }

    @Test
    public void testCoFlatMapTemplate() throws Exception {
        new BaseOperator().coFlatMapTemplate();
    }

    @Test
    public void testIterateTemplate() throws Exception {
        new BaseOperator().iterateTemplate();
    }

    @Test
    public void testTask() {
        System.out.println(getClass().getResource("/"));
        new BaseOperator().task();
    }

}
