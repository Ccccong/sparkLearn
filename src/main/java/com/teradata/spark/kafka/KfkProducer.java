package com.teradata.spark.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author ccc
 * kafka producer properties
 */
public class KfkProducer extends Thread {
    private String topic;
    private Producer<Integer, String> producer;
    private final Boolean isAsync;


    public KfkProducer(String topic, Boolean isAsync) {
        this.topic = topic;
        this.isAsync = isAsync;

        Properties properties = new Properties();

        properties.put("bootstrap.servers", KfkProperties.BROKER_lIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<Integer, String>(properties);

    }


    @Override
    public void run() {
        super.run();
        int messageNo = 1;
        while (true) {
            String message = "message_" + messageNo;
            long startTime = System.currentTimeMillis();
            producer.send(new ProducerRecord<Integer, String>(topic, messageNo, message), new DemoCallback(startTime, messageNo, message));
            if (isAsync) {
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, message), new DemoCallback(startTime, messageNo, message));
            } else {
                try {
                    producer.send(new ProducerRecord<Integer, String>(topic, messageNo, message)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + message + ")");
                } catch (Exception  e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }

    }
}

class DemoCallback implements Callback {
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallback(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }

}
