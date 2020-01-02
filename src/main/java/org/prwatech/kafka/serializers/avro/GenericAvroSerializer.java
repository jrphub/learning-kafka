package org.prwatech.kafka.serializers.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class GenericAvroSerializer {

    private static ByteArrayOutputStream out = new ByteArrayOutputStream();

    public GenericAvroSerializer() {
        throw new IllegalStateException("Inside GenericAvroSerializer");
    }

    public static <T extends GenericRecord> byte[] toBytes(T data) {
        serialize(data, out);
        return out.toByteArray();
    }

    private static <T extends GenericRecord> void serialize(T data, ByteArrayOutputStream out) {
        GenericAvroSerializer.serializeCollection(Collections.singletonList(data), out);
    }

    private static <T extends GenericRecord> void serializeCollection(
            List<T> data, OutputStream out) {
        if (data == null || data.isEmpty()) {
            return;
        }

        GenericRecord record = data.iterator().next();

        try (DataFileWriter<GenericRecord> dataFileWriter =
                     new DataFileWriter<>(new GenericDatumWriter<>())) {
            dataFileWriter.create(record.getSchema(), out);
            for (GenericRecord rec : data ) {
                dataFileWriter.append(rec);
            }
            dataFileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
