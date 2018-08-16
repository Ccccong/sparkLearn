package com.teradata.spark.kafka;

/**
 *kafka api 测试
 */
public class KafkaClientAPP {
    public static void main(String[] args) {

        System.out.println("start kafka consumer");
        new KafkaConsume().start();
        System.out.println("start kafka producer");
        new KafkaProducer(KafkaProperties.TOPIC,true).start();

    }
}