package org.prwatech.kafka.deserializers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class CustomDeserializer<T> implements Deserializer<T> {

    private ObjectMapper mapper;

    @Override
    public void configure(Map configs, boolean isKey) {
        mapper = new ObjectMapper();
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        } else {
            try {
                return mapper.readValue(data, new TypeReference<T>() {
                });
            } catch (IOException e) {
                throw new SerializationException(
                        "Can't deserialize data [" + Arrays.toString(data) +
                                "] from topic [" + topic + "]", e);
            }
        }
    }
}
