package org.prwatech.kafka.deserializers.avro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

public class KafkaAvroDeserializerHelper {

    public static <T extends GenericRecord> T fromBytes(byte[] data)
            throws IllegalAccessException, InstantiationException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        Collection<? extends GenericRecord> dataCollection
                = KafkaAvroDeserializerHelper.deserializeCollection(in, ArrayList.class);
        if (dataCollection == null || dataCollection.isEmpty()) {
            return null;
        }
        return (T) dataCollection.iterator().next();
    }

    private static Collection<? extends GenericRecord> deserializeCollection
            (InputStream in, Class<?> collectionClass) throws InstantiationException, IllegalAccessException {
        Collection<GenericRecord> records;
        records = KafkaAvroDeserializerHelper.getCollectionInstance(collectionClass);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileStream =
                new DataFileStream<>(in, datumReader)) {
            while (dataFileStream.hasNext()) {
                records.add(dataFileStream.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    private static <T extends GenericRecord> Collection<T> getCollectionInstance
            (Class<?> collectionClass) throws IllegalAccessException, InstantiationException {
        if (collectionClass == null) {
            return new ArrayList<>();
        }
        return (Collection<T>) collectionClass.newInstance();
    }
}
