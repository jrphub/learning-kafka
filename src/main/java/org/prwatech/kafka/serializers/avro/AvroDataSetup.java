package org.prwatech.kafka.serializers.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroDataSetup {

    public static GenericRecord setupGenericRecord() {
        String employeeSchema="{\"namespace\": \"employee.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Employee\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"employeeId\", \"type\": \"int\"},\n" +
                "    {\"name\": \"employeeName\", \"type\": \"string\"}\n" +
                "  ]\n" +
                "}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(employeeSchema);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("employeeId", 101L);
        genericRecord.put("employeeName", "prwatech");

        return genericRecord;
    }
}
