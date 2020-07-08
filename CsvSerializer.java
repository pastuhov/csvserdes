package org.example;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class CsvSerializer<T> implements Serializer<T> {
    private final CsvMapper mapper = new CsvMapper();
    private CsvSchema schema;
    private ObjectWriter writer;

    private Class<T> clazz;

    public CsvSerializer(Class<T> clazz) {
        this.clazz = clazz;
        this.schema = mapper.schemaFor(clazz)
                //.withSkipFirstDataRow(true)
                .withColumnSeparator(',');
        this.writer = mapper.writerFor(clazz).with(schema);
    }
    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return writer.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing CSV message", e);
        }
    }

    @Override
    public void close() {
    }
}
