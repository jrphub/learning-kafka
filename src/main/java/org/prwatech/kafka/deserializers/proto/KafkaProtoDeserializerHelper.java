package org.prwatech.kafka.deserializers.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class KafkaProtoDeserializerHelper<T> {
    private Parser<T> parser;

    public KafkaProtoDeserializerHelper(Parser<T> parser) {
        this.parser = parser;
    }

    public KafkaProtoDeserializerHelper() {
    }

    public T deserialize(byte[] data) throws InvalidProtocolBufferException {
        return parser.parseFrom(data);
    }
}
