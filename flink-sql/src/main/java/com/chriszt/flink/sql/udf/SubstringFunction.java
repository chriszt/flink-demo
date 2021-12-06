package com.chriszt.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SubstringFunction extends ScalarFunction {

    public SubstringFunction() {
        System.out.println("++++ SubstringFunction() ++++");
    }

    public String eval(String s, Integer begin, Integer end) {
        System.out.println("--- " + s + ", " + begin + ", " + end + " ---");
        return s.substring(begin, end);
    }

}
