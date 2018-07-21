package com.teradata.spark.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author ccc
 * kafka producer properties
 */
public class KafkaProducer {
    private  String topic;
    private Producer<String,Integer> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;
        Properties properties=new Properties();
        properties.put("metadata.broker.list",KafkaProperties.BROKER_lIST);

        producer=new Producer<String, Integer>(new ProducerConfig(properties));

    }

    public static void main(String[] args) {
        System.out.println("hello world and ccc");
    }
}
