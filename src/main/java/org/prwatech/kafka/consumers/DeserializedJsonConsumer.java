package org.prwatech.kafka.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.prwatech.kafka.beans.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DeserializedJsonConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DeserializedJsonConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "employee-json-serde";

    private static Consumer<Long, Employee> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "jsonSerDeConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.prwatech.kafka.deserializers.json.KafkaJsonDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Create the consumer using props.
        final Consumer<Long, Employee> consumer =
                new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    public static void main(String[] args) {
        logger.info("Consuming messages from kafka topic " + TOPIC);
        final Consumer<Long, Employee> consumer = createConsumer();
        while (true) {
            final ConsumerRecords<Long, Employee> consumerRecords =
                    consumer.poll(Duration.ofMillis(10000));
            consumerRecords.forEach(record -> {
                System.out.printf(
                        "Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                logger.error("Commit failed", e);
            }
        }
    }
}
