package com.chriszt.flink.stream.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KfkConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "srv1:9092,srv2:9092,srv3:9092,");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> kfsSrc = new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), props);
        kfsSrc.setStartFromEarliest();
        DataStream<String> ds = env.addSource(kfsSrc);
        ds.print("ds");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
