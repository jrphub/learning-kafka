package org.prwatech.kafka.serializers.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class CustomJsonSerDe<T> {

    private Class<T> serializedClass;

    public CustomJsonSerDe(Class<T> serializedClass) {
        this.serializedClass = serializedClass;
    }

    public CustomJsonSerDe() {
    }

    public Class<T> getSerializedClass() {
        return serializedClass;
    }

    public void setSerializedClass(Class<T> serializedClass) {
        this.serializedClass = serializedClass;
    }

    public byte[] serialize(T obj) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsBytes(obj);
    }

    public T deSerialize(byte[] data) throws IOException {
        return new ObjectMapper().readValue(data, serializedClass);
    }
}
