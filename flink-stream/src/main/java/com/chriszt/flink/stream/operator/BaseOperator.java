package com.chriszt.flink.stream.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseOperator {

    public static Logger log = LoggerFactory.getLogger(BaseOperator.class);

    public void readTextFile(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.readTextFile(filePath);
        dss.print();

        env.execute();
    }

    public void readFile(String filePath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TextInputFormat fmt = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
//        DataStreamSource<String> dds = env.readFile(fmt, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000, typeInfo);
        DataStreamSource<String> dds = env.readFile(fmt, filePath, FileProcessingMode.PROCESS_ONCE, 10000, typeInfo);
        dds.print();

        env.execute();
    }

    public void writeToScreen() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);
        DataStreamSource<Long> dds = env.fromElements(1L, 21L, 22L);
        dds.print("aaa");

        env.execute();
    }

    public void mapTask() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> ds1 = env.fromSequence(1, 5);
        DataStream<Tuple2<Long, Long>> ds2 = ds1.map(new MapFunction<Long, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Long values) {
                return new Tuple2<Long, Long>(values, values * 1000);
            }
        });
        ds2.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void filterTemplate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> ds1 = env.fromSequence(1L, 5L);
        DataStream<Long> ds2 = ds1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                System.out.println(value);
                if (value == 2L || value == 4L) {
                    return false;
                }
                return true;
            }
        });
        ds2.print();

        env.execute();
    }

    public void keyByTask(List<Trade> lst) {
//        List<Tuple2<Integer, Integer>> lst = new ArrayList<Tuple2<Integer, Integer>>();
//        lst.add(new Tuple2<>(1, 11));
//        lst.add(new Tuple2<>(1, 22));
//        lst.add(new Tuple2<>(3, 33));
//        lst.add(new Tuple2<>(5, 55));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Trade> ds = env.fromCollection(lst);
//        ds.print("ds");

        KeyedStream<Trade, String> ks = ds.keyBy(new KeySelector<Trade, String>() {
            @Override
            public String getKey(Trade value) throws Exception {
                return value.getCardNum();
            }
        });
        ks.print("ks");

//        DataStream<Tuple2<Integer, Integer>> ds1 = env.fromCollection(lst);
//        KeyedStream<Tuple2<Integer, Integer>, Tuple> ks1 = ds1.keyBy(0);
//        KeyedStream<Tuple2<Integer, Integer>, Integer> ks1 = ds1.keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
//            @Override
//            public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
//                return value.f0;
//            }
//        });
//        ks1.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void reduceTask(List<Trade> lst) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Trade> ds1 = env.fromCollection(lst);
        DataStream<Trade> ds2 = ds1.keyBy(new KeySelector<Trade, String>() {
            @Override
            public String getKey(Trade value) throws Exception {
                return value.getCardNum();
            }
        }).reduce(new ReduceFunction<Trade>() {
            @Override
            public Trade reduce(Trade value1, Trade value2) throws Exception {
//                System.out.println(value1 + "  " + value2);
                return new Trade(value1.getCardNum(), value1.getTrade() + value2.getTrade(), "----");
            }
        });
        ds2.print("ds2");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void aggTask(List<Trade> lst) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Trade> ds = env.fromCollection(lst);
        KeyedStream<Trade, String> ks = ds.keyBy(Trade::getCardNum);
//        ks.print("ks");
//        ks.sum("trade").print("sum");
//        ks.min("trade").print("min");
        ks.minBy("trade").print("minBy");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * https://blog.csdn.net/lixinkuan328/article/details/106609817
     */
    public void sideOutputTemplate() throws Exception {
        List<Tuple3<Integer, String, String>> lst = new ArrayList<>();
        lst.add(new Tuple3<>(1, "1", "AAA"));
        lst.add(new Tuple3<>(2, "2", "AAA"));
        lst.add(new Tuple3<>(3, "3", "AAA"));
        lst.add(new Tuple3<>(1, "1", "BBB"));
        lst.add(new Tuple3<>(2, "2", "BBB"));
        lst.add(new Tuple3<>(3, "3", "BBB"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<Tuple3<Integer, String, String>> aTag = new OutputTag<Tuple3<Integer, String, String>>("aTag"){};
        OutputTag<Tuple3<Integer, String, String>> bTag = new OutputTag<Tuple3<Integer, String, String>>("bTag"){};

        DataStream<Tuple3<Integer, String, String>> ds1 = env.fromCollection(lst);
        SingleOutputStreamOperator<Tuple3<Integer, String, String>> procStream = ds1.process(new ProcessFunction<Tuple3<Integer, String, String>, Tuple3<Integer, String, String>>() {
            @Override
            public void processElement(Tuple3<Integer, String, String> value, Context ctx, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                if (value.f2.equals("AAA")) {
                    ctx.output(aTag, value);
                } else if (value.f2.equals("BBB")) {
                    ctx.output(bTag, value);
                } else {
                    out.collect(value);
                }
            }
        });
        procStream.getSideOutput(aTag).print("A Stream");
        procStream.getSideOutput(bTag).print("B Stream");
        procStream.print("Other Stream");

        env.execute();
    }

    public void projectTemplate() throws Exception {
        List<Tuple3<String, Integer, String>> lst = new ArrayList<>();
        lst.add(new Tuple3<>("185xxxx", 899, "??????"));
        lst.add(new Tuple3<>("155xxxx", 1199, "??????"));
        lst.add(new Tuple3<>("138xxxx", 19, "??????"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> ds1 = env.fromCollection(lst);
        DataStream<Tuple2<String, String>> ds2 = ds1.<Tuple2<String, String>>project(2, 0);
        ds2.print();

        env.execute();
    }

    public void unionTemplate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> ds1 = env.fromSequence(1, 2);
        DataStream<Long> ds2 = env.fromSequence(1001, 1002);
        DataStream<Long> ds3 = ds1.union(ds2);
        ds3.print();

        env.execute();
    }

    public void coMapTemplate() throws Exception {
        List<Long> lst1 = new ArrayList<>();
        lst1.add(1L);
        lst1.add(2L);

        List<String> lst2 = new ArrayList<>();
        lst2.add("www huawei com chriszt");
        lst2.add("hello chriszt");
        lst2.add("hello flink");
        lst2.add("hello java");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> ds1 = env.fromCollection(lst1);
        DataStream<String> ds2 = env.fromCollection(lst2);
        ConnectedStreams<Long, String> cs = ds1.connect(ds2);
        DataStream<String> ds3 = cs.map(new CoMapFunction<Long, String, String>() {
            @Override
            public String map1(Long value) throws Exception {
//                System.out.println(value);
                return "??????Long???: " + value;
            }

            @Override
            public String map2(String value) throws Exception {
//                System.out.println(value);
                return "??????String???: " + value;
            }
        });
        ds3.print();

        env.execute();
    }

    public void coFlatMapTemplate() throws Exception {
        List<Long> lst1 = new ArrayList<>();
        lst1.add(1L);
        lst1.add(2L);

        List<String> lst2 = new ArrayList<>();
        lst2.add("www huawei com chriszt");
        lst2.add("hello chriszt");
        lst2.add("hello flink");
        lst2.add("hello java");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> ds1 = env.fromCollection(lst1);
        DataStream<String> ds2 = env.fromCollection(lst2);
        ConnectedStreams<Long, String> cs = ds1.connect(ds2);
        DataStream<String> ds3 = cs.flatMap(new CoFlatMapFunction<Long, String, String>() {
            @Override
            public void flatMap1(Long value, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });
        ds3.print();

        env.execute();
    }

    public void iterateTemplate() throws Exception {
        List<Tuple2<String, Integer>> lst = new ArrayList<>();
        lst.add(new Tuple2<>("flink", 33));
        lst.add(new Tuple2<>("storm", 32));
        lst.add(new Tuple2<>("spark", 15));
        lst.add(new Tuple2<>("java", 18));
        lst.add(new Tuple2<>("python", 31));
        lst.add(new Tuple2<>("scala", 29));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<Tuple2<String, Integer>> iterateTag = new OutputTag<Tuple2<String, Integer>>("iterateTag"){};
//        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("outputTag"){};

        DataStream<Tuple2<String, Integer>> ds = env.fromCollection(lst);
        IterativeStream<Tuple2<String, Integer>> its = ds.iterate(5000);
        SingleOutputStreamOperator<Tuple2<String, Integer>> soso = its.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Thread.sleep(1000);
                System.out.println("[map] " + value);
                return new Tuple2<>(value.f0, --value.f1);
            }
        }).process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("[prcess] " + value);
                if (value.f1 > 30) {
                    System.out.println(value + " --> iterateTag");
                    ctx.output(iterateTag, value);
                } else {
                    System.out.println(value + " --> outputTag");
//                    ctx.output(outputTag, value);
                    out.collect(value);
                }
            }
        });
        its.closeWith(soso.getSideOutput(iterateTag));
        soso.getSideOutput(iterateTag).print("iterateTag: ");

//        its.closeWith(soso.getSideOutput(outputTag));
//        soso.getSideOutput(outputTag).print("outputTag: ");

//        its.closeWith(soso);
//        soso.print("Main Stream: ");

        env.execute();
    }

    public void task() {
//        List<String> lst = new ArrayList<>();
//        lst.add("aaa");
//        lst.add("bbb");
//        lst.add("ccc");
//
//        String res = String.join("-", (CharSequence) lst);
//        System.out.println(res);

        System.out.println(getClass().getResource("/"));
    }

    public static void main(String[] args) {
        new BaseOperator().task();
    }
}
