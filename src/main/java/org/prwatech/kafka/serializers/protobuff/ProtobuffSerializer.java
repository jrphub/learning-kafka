package org.prwatech.kafka.serializers.protobuff;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

public class ProtobuffSerializer<T extends Message> implements Serializer<T> {
    @Override
    public byte[] serialize(String topic, T message) {
        return message.toByteArray();
    }
}
