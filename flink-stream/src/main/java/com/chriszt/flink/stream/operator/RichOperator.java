package com.chriszt.flink.stream.operator;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RichOperator extends RichFlatMapFunction<Long, Long> {

    public static Logger log = LoggerFactory.getLogger(RichOperator.class);

    @Override
    public void open(Configuration cfg) throws Exception {
        log.info("**** open() ****");
        RuntimeContext rc = getRuntimeContext();
        String taskName = rc.getTaskName();
//        log.info("++++" + taskName + "++++");
        String subTaskName = rc.getTaskNameWithSubtasks();
//        log.info("++++" + subTaskName + "++++");
        int subTaskIndexOf = rc.getIndexOfThisSubtask();
//        log.info("++++" + subTaskIndexOf + "++++");
        int parallel = rc.getNumberOfParallelSubtasks();
//        log.info("++++" + parallel + "++++");
        int attemptNum = rc.getAttemptNumber();
//        log.info("++++" + attemptNum + "++++");
        log.info("\n  [taskName] {}\n  [subTaskName] {}\n  [subTaskIndexOf] {}\n  [parallel] {}\n  [attemptNum] {}",
                taskName, subTaskName, subTaskIndexOf, parallel, attemptNum);
    }

    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        Thread.sleep(5000);
        log.info("**** flatMap() ****");
        out.collect(value);
    }

    @Override
    public void close() throws Exception {
        log.info("**** close() ****");
    }

    public static void gameover1() {
        log.info("**** gameover1() ****");
    }

    public static void gameover2() {
        log.info("**** gameover2() ****");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Long> ds1 = env.fromSequence(1, 5);
        DataStream<Long> ds2 = ds1.flatMap(new RichOperator()).name("name123");
        ds2.print();

        gameover1();
        env.execute();
        gameover2();
    }


}
