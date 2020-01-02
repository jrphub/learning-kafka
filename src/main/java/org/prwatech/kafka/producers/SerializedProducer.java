package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.prwatech.kafka.beans.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class SerializedProducer {
    private static final Logger logger = LoggerFactory.getLogger(SerializedProducer.class);

    private static final String TOPIC = "employee-custom-serde";

    private static KafkaProducer<Long, Employee> kafkaProducer = createProducer();

    private static KafkaProducer<Long, Employee> createProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092";
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer",
                LongSerializer.class.getName());
        props.put("value.serializer",
                "org.prwatech.kafka.serializers.CustomSerializer");
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Employee emp1 = new Employee(101, "prwatech");

        logger.info("Publishing messages to kafka topic " + TOPIC);
        ProducerRecord<Long, Employee> record = new ProducerRecord<>(TOPIC,
                emp1.getEmployeeId(),emp1);
        Future<RecordMetadata> respond =  kafkaProducer.send(record);
        RecordMetadata metadata = respond.get();
        System.out.format("Topic : %s,\n" +
                        "Offset : %d,\n" +
                        "Partition : %d,\n" +
                        "Serialized KeySize : %d,\n" +
                        "Serialized ValueSize : %d,\n" +
                        "timestamp : %d",
                metadata.topic(),
                metadata.offset(),
                metadata.partition(),
                metadata.serializedKeySize(),
                metadata.serializedValueSize(),
                metadata.timestamp());
    }

}
