package com.chriszt.flink.stream.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Properties;

public class KfkProducer {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.fromCollection(Arrays.asList("xxx", "yyy", "zzz"));

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "srv1:9092,srv2:9092,srv3:9092,");
        FlinkKafkaProducer<String> kfkSink = new FlinkKafkaProducer<String>("first", new SimpleStringSchema(), props);

//        ds.print("ds");
        for (int i = 0; i < 3; i++) {
            ds.addSink(kfkSink);
        }


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

//        ds.addSink(kfkSink);
    }

}
