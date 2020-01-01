package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class SimpleProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    private static KafkaProducer<String, String> kafkaProducer = createProducer();

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092,localhost:9093,localhost:9094";
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "words";
        logger.info("Publishing messages to kafka topic " + topic);
        while (true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    "msg_"+System.currentTimeMillis());
            kafkaProducer.send(record);
            Thread.sleep(5000);
        }
    }

}
