package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AsynchronousProducer {
    private static final Logger logger = LoggerFactory.getLogger(AsynchronousProducer.class);

    private static final String TOPIC = "word-asynch";

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
            kafkaProducer.send(record, new DemoProducerCallback());
            Thread.sleep(5000);
        }
    }

    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                //handle the exception
                e.printStackTrace();
            }
        }
    }
}
