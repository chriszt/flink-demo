package com.chriszt.flink.sql.streamintegration;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

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

    // TODO
    public void work4() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<Row> ds = env.fromElements(
                Row.of("zhangsan", 18, "C"),
                Row.of("lisi", 20, "A"),
                Row.of("wangwu", 25, "B"));
        DataStream<Row> ds2 = env.fromElements(Row.of("Alice", 10, "A"));
//        ds.print();

        Table inTab = tabEnv.fromDataStream(ds).as("name", "age", "level");
        tabEnv.createTemporaryView("InputTable", inTab);
//        tabEnv.executeSql("SELECT * FROM InputTable").print();

//        Table outTab = tabEnv.fromDataStream(ds2).as("name", "age", "level");
//        tabEnv.createTemporaryView("OutputTable", outTab);
//        tabEnv.executeSql("SELECT * FROM OutputTable").print();
        tabEnv.executeSql("CREATE TABLE OutputTable(name STRING, age INT, level STRING)");


//        for (String s : tabEnv.listViews()) {
//            System.out.println(s);
//        }

//        tabEnv.from("InputTable").executeInsert("OutputTable").print();

        tabEnv.executeSql("INSERT INTO OutputTable SELECT * FROM InputTable");


//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public void work5() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<User2> ds = env.fromElements(
                new User2("Alice", 4, Instant.ofEpochMilli(1000)),
                new User2("Bob", 6, Instant.ofEpochMilli(1001)),
                new User2("Cindy", 10, Instant.ofEpochMilli(1002)));
//        ds.print();

// === EXAMPLE 1 ===
//        Table tab = tabEnv.fromDataStream(ds);
//        tab.printSchema();
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9)
        // )

// === EXAMPLE 2 ===
//        Table tab = tabEnv.fromDataStream(
//                ds,
//                Schema.newBuilder()
//                        .columnByExpression("proc_time", "PROCTIME()")
//                        .build());
//        tab.printSchema();
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
        // )

// === EXAMPLE 3 ===
//        Table tab = tabEnv.fromDataStream(
//                ds,
//                Schema.newBuilder()
//                        .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
//                        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
//                        .build());
//        tab.printSchema();
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
        //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
        // )

// === EXAMPLE 4 ===
//        Table tab = tabEnv.fromDataStream(
//                ds,
//                Schema.newBuilder()
//                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
//                        .watermark("rowtime", "SOURCE_WATERMARK()")
//                        .build());
//        tab.printSchema();
        // (
        //  `name` STRING,
        //  `score` INT,
        //  `event_time` TIMESTAMP_LTZ(9),
        //  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
        //  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
        // )

// === EXAMPLE 5 ===
        Table tab = tabEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                        .column("event_time", "TIMESTAMP_LTZ(3)")
                        .column("name", "STRING")
                        .column("score", "INT")
                        .watermark("event_time", "SOURCE_WATERMARK()")
                        .build());
        tab.printSchema();
        // (
        //  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
        //  `name` STRING,
        //  `score` INT
        // )

//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public void work6() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple1<User3>> ds = env.fromElements(
                Tuple1.of(new User3("Alice", 4)),
                Tuple1.of(new User3("Bob", 6)),
                Tuple1.of(new User3("Cindy", 10)));
//        ds.print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

// === EXAMPLE 1 ===
//        Table tab = tabEnv.fromDataStream(ds);
//        tab.printSchema();
        // (
        //  `f0` *com.chriszt.flink.sql.streamintegration.User3<`name` STRING, `score` INT>*
        // )

// === EXAMPLE 2 ===
//        Table tab = tabEnv.fromDataStream(
//                ds,
//                Schema.newBuilder()
//                        .column("f0", DataTypes.of(User3.class))
//                        .build())
//                .as("user");
//        tab.printSchema();
        // (
        //  `user` *com.chriszt.flink.sql.streamintegration.User3<`name` STRING, `score` INT>*
        // )

// === EXAMPLE 3 ===
        Table tab = tabEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                        .column(
                                "f0",
                                DataTypes.STRUCTURED(
                                    User3.class,
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("score", DataTypes.INT())))
                        .build()).as("user");
        tab.printSchema();
        // (
        //  `user` *com.chriszt.flink.sql.streamintegration.User3<`name` STRING, `score` INT>*
        // )
    }

    public void work7() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<Long, String>> ds = env.fromElements(
                Tuple2.of(12L, "Alice"),
                Tuple2.of(0L, "Bob"));
//        ds.print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

// === EXAMPLE 1 ===
//        tabEnv.createTemporaryView("MyView", ds);
//        tabEnv.from("MyView").printSchema();
        // (
        //  `f0` BIGINT NOT NULL,
        //  `f1` STRING
        // )

// === EXAMPLE 2 ===
//        tabEnv.createTemporaryView("MyView", ds,
//                Schema.newBuilder()
//                        .column("f0", "BIGINT")
//                        .column("f1", "STRING")
//                        .build());
//        tabEnv.from("MyView").printSchema();
        // (
        //  `f0` BIGINT,
        //  `f1` STRING
        // )

// === EXAMPLE 3 ===
        tabEnv.createTemporaryView("MyView",
                tabEnv.fromDataStream(ds).as("id", "name"));
        tabEnv.from("MyView").printSchema();
        // (
        //  `id` BIGINT NOT NULL,
        //  `name` STRING
        // )
    }

    public void work8() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.executeSql(
                "CREATE TABLE GeneratedTable (" +
                        "name STRING, " +
                        "score INT, " +
                        "event_time TIMESTAMP_LTZ(3), " +
                        "WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND) " +
                        "WITH ('connector'='datagen')");
        Table tab = tabEnv.from("GeneratedTable");
//        tab.printSchema();

// === EXAMPLE 1 ===
//        DataStream<Row> ds = tabEnv.toDataStream(tab);
//        ds.print();

// === EXAMPLE 2 ===
//        DataStream<User2> ds = tabEnv.toDataStream(tab, User2.class);
//        ds.print();

// === EXAMPLE 3 ===
        DataStream<User2> ds = tabEnv.toDataStream(tab,
                DataTypes.STRUCTURED(User2.class,
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT()),
                        DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))));
        ds.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void work9() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

// === EXAMPLE 1 ===
/*
//        DataStream<Row> ds = env.fromElements(
//                Row.of("Alice", 12),
//                Row.of("Bob", 5));
        DataStream<Row> ds = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
//        ds.print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        Table tab = tabEnv.fromChangelogStream(ds);
//        tab.printSchema();
        // (
        //  `f0` STRING,
        //  `f1` INT
        // )

        tabEnv.createTemporaryView("InputTable", tab);
        tabEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print();
*/

// === EXAMPLE 2 ===
        DataStream<Row> ds = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
//        ds.print();
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        Table tab = tabEnv.fromChangelogStream(ds,
                Schema.newBuilder().primaryKey("f0").build(),
                ChangelogMode.upsert());
        tabEnv.createTemporaryView("InputTable", tab);
        tabEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print();
    }

    public void work10() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE GeneratedTable " +
                "(" +
                "  name STRING," +
                "  score INT, " +
                "  event_time TIMESTAMP_LTZ(3)," +
                "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND" +
                ") " +
                "WITH ('connector'='datagen')";
        tabEnv.executeSql(sql);
        Table tab = tabEnv.from("GeneratedTable");

// === EXAMPLE 1 ===
//        Table simpleTab = tabEnv
//                .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
//                .as("name", "score")
//                .groupBy($("name"))
//                .select($("name"), $("score").sum());
//        try {
//            tabEnv.toChangelogStream(simpleTab)
//                    .executeAndCollect()
//                    .forEachRemaining(System.out::println);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

// === EXAMPLE 2 ===
//        DataStream<Row> ds = tabEnv.toChangelogStream(tab);
////        ds.print();
//
//        ds.process(new ProcessFunction<Row, Void>() {
//            @Override
//            public void processElement(Row row, Context context, Collector<Void> collector) throws Exception {
//                System.out.println(row.getFieldNames(true));
//                assert context.timestamp() == row.<Instant>getFieldAs("event_time").toEpochMilli();
//            }
//        });
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

// === EXAMPLE 3 ===
//        DataStream<Row> ds = tabEnv.toChangelogStream(tab,
//                Schema.newBuilder()
//                        .column("name", "STRING")
//                        .column("score", "INT")
//                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
//                        .build());
////        ds.print();
//        ds.process(new ProcessFunction<Row, Void>() {
//            @Override
//            public void processElement(Row row, Context context, Collector<Void> collector) throws Exception {
//                System.out.println(row.getFieldNames(true));
//                System.out.println(context.timestamp());
//            }
//        });
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
// === EXAMPLE 4 ===
        DataStream<Row> ds = tabEnv.toChangelogStream(tab,
                Schema.newBuilder()
                        .column("name", DataTypes.STRING().bridgedTo(StringData.class))
                        .column("score", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP_LTZ(3).bridgedTo(Long.class))
                        .build());
//        ds.print();
        ds.process(new ProcessFunction<Row, Void>() {
            @Override
            public void processElement(Row row, Context context, Collector<Void> collector) throws Exception {
                System.out.println(row);
            }
        });
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
