package com.teradata.spark.kafka;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;


/**
 * @author ccc
 * kafka producer properties
 */
public class KafkaProducer extends Thread {
    private String topic;
    private Producer<String, String> producer;
    private final Boolean isAsync;


    public KafkaProducer(String topic, Boolean isAsync) {
        System.out.println("start producer");
        this.topic = topic;
        this.isAsync = isAsync;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
        properties.put("acks", KafkaProperties.ACKS);
        properties.put("retries", KafkaProperties.RETRIES);
        properties.put("batch.size", KafkaProperties.BATCH_SIZE);
        properties.put("linger.ms", KafkaProperties.LINGER_MS);
        properties.put("buffer.memory", KafkaProperties.BUFFER_MEMORY);
        properties.put("key.serializer", KafkaProperties.KEY_SERIALIZER);
        properties.put("value.serializer", KafkaProperties.VALUE_SERIALIZER);

        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
    }


    @Override
    public void run() {
        super.run();
        int messageNo = 0;
        while (true) {
            String message = "message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) {
                producer.send(new ProducerRecord<String, String>(topic, messageNo + "", message), new DemoCallback(startTime, messageNo, message));
            } else {
                try {
                    producer.send(new ProducerRecord<String, String>(topic, messageNo + "", message)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + message + ")");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
