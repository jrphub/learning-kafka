package org.prwatech.kafka.serializers.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJsonSerializer<T> implements Serializer<T> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaJsonSerializer.class);

    @Override
    public byte[] serialize(String ignore, T t) {
        GenericJsonMessage<T> message = new GenericJsonMessage<>(t.getClass(), t);

        logger.info("GenericJsonMessage :" + message.toString());

        try {
            return new CustomJsonSerDe().serialize(message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
