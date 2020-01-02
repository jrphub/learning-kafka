package org.prwatech.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.prwatech.kafka.beans.Employee;
import org.prwatech.kafka.serializers.json.KafkaJsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SerializedJsonProducer {
    private static final Logger logger = LoggerFactory.getLogger(SerializedJsonProducer.class);
    private static final String TOPIC = "employee-json-serde";

    private static KafkaProducer<Long,byte[]> createKafkaProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.prwatech.kafka.serializers.json.KafkaJsonSerializer");

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000"); //10Kb batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5000"); //5 sec linger.ms

        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<Long, byte[]> kafkaProducer = createKafkaProducer();
        Employee emp1 = new Employee(101, "prwatech");

        KafkaJsonSerializer<Employee> serializer = new KafkaJsonSerializer<>();
        byte[] empBytes = serializer.serialize(null,emp1);

        logger.info("Publishing messages to Kafka topic :" + TOPIC);
        ProducerRecord<Long, byte[]> record = new ProducerRecord<>(TOPIC,emp1.getEmployeeId(),
                empBytes);
        Future<RecordMetadata> respond = kafkaProducer.send(record);
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
