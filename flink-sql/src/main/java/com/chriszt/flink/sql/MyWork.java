package com.chriszt.flink.sql;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MyWork {

    public void work1(String filePath) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> rawStream = env.readTextFile(filePath);
//        rawStream.print("rawStream");

        DataStream<User> tabStream = rawStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String s) throws Exception {
                String[] token = s.split(",");
                return new User(Integer.parseInt(token[0]), token[1], Integer.parseInt(token[2]));
            }
        });
//        tabStream.print("tabStream");

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        Schema schema1 = Schema.newBuilder()
                .column("id", "int")
                .column("name", "STRING")
                .column("age", "int")
                .build();
        Table tab1 = tabEnv.fromDataStream(tabStream, schema1);
        tabEnv.createTemporaryView("tab1", tab1);
        tabEnv.from("tab1").printSchema();


        TableResult tabRet1 = tabEnv.executeSql("select * from tab1");
        tabRet1.print();
/*
+----+-------------+--------------------------------+-------------+
| op |          id |                           name |         age |
+----+-------------+--------------------------------+-------------+
| +I |           3 |                         yanlin |          35 |
| +I |           4 |                        chriszt |          25 |
| +I |           2 |                           lisi |          20 |
| +I |           1 |                       zhangsan |          18 |
+----+-------------+--------------------------------+-------------+
4 rows in set
*/

        TableResult tabRet2 = tabEnv.executeSql("select * from tab1 where id=3 or name='chriszt'");
        tabRet2.print();
/*
+----+-------------+--------------------------------+-------------+
| op |          id |                           name |         age |
+----+-------------+--------------------------------+-------------+
| +I |           4 |                        chriszt |          25 |
| +I |           3 |                         yanlin |          35 |
+----+-------------+--------------------------------+-------------+
2 rows in set
*/



        try {
            JobExecutionResult exeRet = env.execute();
            System.out.println("Execution Time: " + exeRet.getNetRuntime() + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
