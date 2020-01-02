package org.prwatech.kafka.deserializers.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;

public class GenericAvroDeserializer<T extends GenericRecord> implements Deserializer<T> {
    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return KafkaAvroDeserializerHelper.fromBytes(data);
        } catch (Exception e) {
            throw new SerializationException(
                    "Can't deserialize json data [" + Arrays.toString(data)
                            + "] from topic [" + topic + "]", e);
        }
    }
}
