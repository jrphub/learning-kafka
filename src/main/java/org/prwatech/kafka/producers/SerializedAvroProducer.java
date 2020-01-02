package org.prwatech.kafka.producers;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.prwatech.kafka.serializers.avro.AvroDataSetup;
import org.prwatech.kafka.serializers.avro.GenericAvroSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SerializedAvroProducer {
    private static final Logger logger = LoggerFactory.getLogger(SerializedAvroProducer.class);
    private static final String TOPIC = "employee-avro-serde";

    private static KafkaProducer<Long,byte[]> createKafkaProducer() {
        Properties props = new Properties();
        String brokers = "localhost:9092";

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000"); //10Kb batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5000"); //5 sec linger.ms

        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GenericRecord genericRecord = AvroDataSetup.setupGenericRecord();
        byte[] avroBytes = GenericAvroSerializer.toBytes(genericRecord);

        logger.info("Publishing messages to kafka topic " + TOPIC);
        ProducerRecord<Long, byte[]> record = new ProducerRecord<>(TOPIC,
                (Long) genericRecord.get("employeeId"), avroBytes);

        KafkaProducer<Long, byte[]> kafkaProducer = createKafkaProducer();
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
