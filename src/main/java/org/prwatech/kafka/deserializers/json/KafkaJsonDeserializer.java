package org.prwatech.kafka.deserializers.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.prwatech.kafka.serializers.json.CustomJsonSerDe;
import org.prwatech.kafka.serializers.json.GenericJsonMessage;

import java.io.IOException;
import java.util.Arrays;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private Class<T> targetType = null;

    public KafkaJsonDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    public KafkaJsonDeserializer() {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        } else {
            try {
                if (targetType != null) {
                    return new CustomJsonSerDe<T>(targetType).deSerialize(data);
                } else {
                    GenericJsonMessage<T> message = new CustomJsonSerDe<>(GenericJsonMessage.class)
                            .deSerialize(data);
                    return (T) new ObjectMapper()
                            .convertValue(message.getPayload(), message.getClassName());
                }
            } catch (IOException e) {
                throw new SerializationException(
                        "Can't deserialize json data [" + Arrays.toString(data)
                        + "] from topic [" + topic + "]", e);
            }
        }
    }
}
