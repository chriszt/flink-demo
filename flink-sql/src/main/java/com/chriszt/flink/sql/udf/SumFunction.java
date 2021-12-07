package com.chriszt.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SumFunction extends ScalarFunction {

    public Integer eval(Integer a, Integer b) {
        System.out.println("---- eval(Integer a, Integer b) ----");
        return a + b;
    }

    public Integer eval(String a, String b) {
        System.out.println("---- eval(String a, String b) ----");
        return Integer.parseInt(a) + Integer.parseInt(b);
    }

    public Integer eval(Double... d) {
        System.out.println("---- eval(Double... d) ----");
        double ret = 0;
        for (double val : d) {
            ret += val;
        }
        return (int) ret;
    }

}
