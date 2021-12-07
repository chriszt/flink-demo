package com.chriszt.flink.sql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class OverloadedFunction extends ScalarFunction {

    public long eval(long a, long b) {
        return a + b;
    }

    public @DataTypeHint("DECIMAL(12, 3)") BigDecimal eval(double a, double b) {
        return BigDecimal.valueOf(a + b);
    }

    @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
    public Row eval(int i) {
        return Row.of(String.valueOf(i), Instant.ofEpochSecond(i));
    }

}
