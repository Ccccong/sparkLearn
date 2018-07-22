package com.teradata.spark.kafka;

/**
 *kafka api 测试
 */
public class KfkClientAPP {
    public static void main(String[] args) {

        System.out.println("start kafka test");

        new KfkProducer(KfkProperties.TOPIC,true).start();
    }
}