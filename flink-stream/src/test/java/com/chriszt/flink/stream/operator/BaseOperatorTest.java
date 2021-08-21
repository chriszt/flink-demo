package com.chriszt.flink.stream.operator;

import org.junit.Before;
import org.junit.Test;

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
    public void testMapTemplate() throws Exception {
        new BaseOperator().mapTemplate();
    }

    @Test
    public void testFilterTemplate() throws Exception {
        new BaseOperator().filterTemplate();
    }

    @Test
    public void testKeyByTemplate() throws Exception {
        new BaseOperator().keyByTemplate();
    }

    @Test
    public void testReduceTemplate() throws Exception {
        new BaseOperator().reduceTemplate();
    }

    @Test
    public void testAggreTemplate() throws Exception {
        new BaseOperator().aggreTemplate();
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

}
