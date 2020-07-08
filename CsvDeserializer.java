package org.example;

import com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.common.errors.SerializationException;

public class CsvDeserializer<T> implements Deserializer<T> {
    private final CsvMapper mapper = new CsvMapper();
    private CsvSchema schema;
    private ObjectReader reader;

    private Class<T> clazz;

    public CsvDeserializer(Class<T> clazz) {
        this.mapper.enable(CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE);
        this.clazz = clazz;
        this.schema = mapper.schemaFor(clazz)
                //.withSkipFirstDataRow(true)
                .withColumnSeparator(',');
        this.reader = mapper.readerFor(clazz).with(schema);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {

        if (bytes == null)
            return null;

        T data;
        try {
            data = reader.readValue(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}
