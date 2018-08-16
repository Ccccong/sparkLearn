package com.teradata.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * anthor ccc
 * kafka消费者
 */
class KafkaConsume extends Thread{

    private org.apache.kafka.clients.consumer.KafkaConsumer consumer;


    public  KafkaConsume() {
        Properties props = new Properties();
        //设置集群，不必是群集中服务器的详尽列表（如果服务器关闭需要制定多个）
        props.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
        //设置consumer的group.id
        props.put("group.id", "testgroup0");
        //自动提交为true
        props.put("enable.auto.commit", "true");
        //配置控制时间间隔来自动提交偏移
        props.put("auto.commit.interval.ms", "1000");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);



    }

    @Override
    public void run() {
        super.run();
        //订阅主题test0和test1作为配置group.id=testgroup0的一部分
        consumer.subscribe(Arrays.asList("test0", "test1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    public static void main(String[] args) {
        new KafkaConsume().run();
    }
}


