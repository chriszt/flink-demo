package com.chriszt.flink.sql;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyWork {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    public void work1(String filePath) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> rawStream = env.readTextFile(filePath);
//        rawStream.print("rawStream");
        DataStream<User> userStream = rawStream.map(s -> {
            String[] tokens = s.split(",");
            return new User(Integer.parseInt(tokens[0]), tokens[1], tokens[2], Integer.parseInt(tokens[3]));
        });
//        userStream.print("userStream");

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        Schema userSchema = Schema.newBuilder()
                                  .column("id", "INTEGER")
                                  .column("clazz", "STRING")
                                  .column("name", "STRING")
                                  .column("age", "INTEGER")
                                  .columnByExpression("inTime", "PROCTIME()")
                                  .build();
        tabEnv.createTemporaryView("UserTab", userStream, userSchema);

        tabEnv.executeSql(FlinkSQL.RULE1).print();
//        tabEnv.executeSql(FlinkSQL.RULE2).print();
        tabEnv.executeSql(CepSQL.RULE1).print();


        try {
            JobExecutionResult res = env.execute();
            System.out.println("NetRuntime: " + res.getNetRuntime() + "ms");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public void work2() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<String> ds = env.fromElements("Alice", "Bob", "John");
//        ds.print();

        Table dsTab = tabEnv.fromDataStream(ds);
//        dsTab.printSchema();

        tabEnv.createTemporaryView("InputTable", dsTab);
        Table resultTable = tabEnv.sqlQuery("select UPPER(f0) from InputTable");

        DataStream<Row> resultStream1 = tabEnv.toDataStream(dsTab);
        resultStream1.print();
        DataStream<Row> resultStream2 = tabEnv.toDataStream(resultTable);
        resultStream2.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void work3() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<Row> ds = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));
//        ds.print();

        Table inputTable = tabEnv.fromDataStream(ds).as("name", "score");

        tabEnv.createTemporaryView("InputTable", inputTable);

        Table resultTable = tabEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name");

        DataStream<Row> resultStream = tabEnv.toChangelogStream(resultTable);
        resultStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        String filePath = MyWork.class.getResource("/tab1.csv").getPath();
        String filePath = "/home/yl/proj/flink-demo/sql-cli/userTab.csv";
        new MyWork().work1(filePath);
    }

}
