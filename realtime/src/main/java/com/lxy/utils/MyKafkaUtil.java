package com.lxy.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String brokers = "node1:9092,node2:9092,node3:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";


    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }
}
