package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BatchProducer {
    private static final Logger logger = LoggerFactory.getLogger(BatchProducer.class);

    private static final String TOPIC = "word-batch";

    private static KafkaProducer<Long, String> kafkaProducer = createProducer();

    private static KafkaProducer<Long, String> createProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000"); //10Kb batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5000"); //5 sec linger.ms

        return new KafkaProducer<Long, String>(props);
    }

    public static void main(String[] args) throws InterruptedException {
        logger.info("Publishing messages to kafka topic " + TOPIC);
        while(true) {
            //define a key
            long currTime = System.currentTimeMillis();
            long key = Long.valueOf(String.valueOf(currTime).substring(0,2));
            //long key = Long.valueOf(String.valueOf(currTime))%3;
            logger.info("Key : {}", key);
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC,
                    key, "msg_"+System.currentTimeMillis());
            kafkaProducer.send(record);
            Thread.sleep(1000);
        }
    }
}
