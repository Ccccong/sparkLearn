package com.teradata.spark.kafka;

import java.util.ArrayList;
import java.util.List;

/**
 * @author CCC
 * kafka常用配置文件
 */
public class KfkProperties {
    final static String ZK = "192.168.221.12:2181";
    final static String TOPIC = "test0";
    final static String BOOTSTRAP_SERVERS = "192.168.221.12:9092,192.168.221.12:9092,192.168.221.12:9092";
    final static String ACKS = "all";
    final static Integer RETRIES = 0;
    final static Integer BATCH_SIZE = 16384;
    final static Integer LINGER_MS = 1;
    final static Integer BUFFER_MEMORY = 33554432;
    final static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    final static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
}
