package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;


public class ProducerWCustPartition {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWCustPartition.class);

    private static final String TOPIC = "msg";

    private static KafkaProducer<String, String> kafkaProducer = createProducer();

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092";
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer",
                StringSerializer.class.getName());
        props.put("value.serializer",
                StringSerializer.class.getName());
        props.put("batch.size", "10000"); //10Kb batch size
        props.put("linger.ms", "5000"); //5 sec linger.ms
        props.put("partitioner.class","org.prwatech.kafka.partitioner.CustomPartitioner");
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws InterruptedException {

        logger.info("Publishing messages to kafka topic " + TOPIC);
        while (true) {
            //define a key
            String key = UUID.randomUUID().toString();
            logger.info("Key : " + key);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key,
                    System.currentTimeMillis()+"_msg");
            kafkaProducer.send(record);
            Thread.sleep(1000);
        }
    }

}
