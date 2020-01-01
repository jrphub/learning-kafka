package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SynchronousProducer {
    private static final Logger logger = LoggerFactory.getLogger(SynchronousProducer.class);

    private static final String TOPIC = "word-synch";

    private static KafkaProducer<String, String> kafkaProducer = createProducer();

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(props);
    }

    public static void main(String[] args) throws InterruptedException {
        logger.info("Publishing messages to kafka topic " + TOPIC);
        while(true) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "msg_"+System.currentTimeMillis());
            try {
                kafkaProducer.send(record).get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
    }
}
