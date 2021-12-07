package com.chriszt.flink.sql.udf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.apache.flink.table.api.Expressions.*;

public class MainTask {

    public void task1() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<Row> ds = env.fromElements(
                Row.of("Alice", 18),
                Row.of("Bob", 17),
                Row.of("Cindy", 20));
//        ds.print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        tabEnv.createTemporaryView("MyTable", ds, $("name"), $("age"));
//        tabEnv.from("MyTable").printSchema();
//        tabEnv.from("MyTable").select(call(SubstringFunction.class, $("name"), 0, 2)).execute().print();

        tabEnv.createTemporarySystemFunction("substring", SubstringFunction.class);
//        tabEnv.from("MyTable").select(call("substring", $("name"), 0, 2)).execute().print();

//        tabEnv.sqlQuery("SELECT substring(name, 0, 2) FROM MyTable").execute().print();
        tabEnv.executeSql("SELECT substring(name, 0, 2) FROM MyTable").print();
    }

    public void task2() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.createTemporarySystemFunction("mysum", new SumFunction());

        tabEnv.executeSql("SELECT mysum(1, 2)").print();
        tabEnv.executeSql("SELECT mysum('1', '2')").print();
        tabEnv.executeSql("SELECT mysum(1, 2, 3, 4)").print();
    }

    public void task3() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.createTemporarySystemFunction("overloaded", new OverloadedFunction());

        tabEnv.executeSql("SELECT overloaded(1, 2)").print();
        tabEnv.executeSql("SELECT overloaded(1.1, 2.2)").print();
        tabEnv.executeSql("SELECT overloaded(1)").print();
    }

    public static void main(String[] args) {
        MainTask mainTask = new MainTask();
        mainTask.task3();
    }
}
