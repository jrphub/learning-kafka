package org.prwatech.kafka.deserializers.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.prwatech.kafka.beans.CompanyResources;

import java.util.Arrays;

public class ProtobuffDeserializer<T extends Message> implements Deserializer<T> {

    private Parser<T> parser;

    public ProtobuffDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    public ProtobuffDeserializer() {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        if (parser == null) {
            //We have to provide schema as proto doesn't carry schema with it.
            parser = (Parser<T>) CompanyResources.Employee.parser();
        }
        try {
            return new KafkaProtoDeserializerHelper<T>(parser).deserialize(data);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException(
                    "Can't deserialize json data [" + Arrays.toString(data)
                            + "] from topic [" + topic + "]", e);
        }
    }
}
