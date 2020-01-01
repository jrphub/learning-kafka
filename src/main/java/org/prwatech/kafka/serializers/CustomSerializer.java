package org.prwatech.kafka.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerializer<T> implements Serializer<T> {

    private ObjectMapper mapper;

    @Override
    public void configure(Map configs, boolean isKey) {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            if (data == null) {
                return null;
            } else {
                return mapper.writeValueAsBytes(data);
            }

        } catch (Exception e) {
            throw new SerializationException("Error when serializing" +
                    "customer to byte[]" + e);
        }
    }

}
