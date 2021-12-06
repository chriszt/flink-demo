package com.chriszt.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

import static org.apache.flink.table.api.Expressions.$;

public class SQLDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings es = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        DataStream<String> ds = env.fromElements("aaa", "bbb", "ccc");
        ds.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
