package com.teradata.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 源码里面有解释
 */
public class KafkaClientSimpleTest {

    public KafkaClientSimpleTest() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.221.12:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++){
            System.out.println("start send ... ...");
            producer.send(new ProducerRecord<String, String>("test0", Integer.toString(i), Integer.toString(i)));
            System.out.println("send over");
        }
        producer.close();
    }

    public static void main(String[] args) {
        new KafkaClientSimpleTest();
    }
}
