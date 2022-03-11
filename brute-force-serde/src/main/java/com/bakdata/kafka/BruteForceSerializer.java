package com.bakdata.kafka;

import java.util.Map;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

@NoArgsConstructor
public class BruteForceSerializer<T> implements Serializer<T> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {}

    @Override
    public byte[] serialize(final String topic, final T data) {
        throw new SerializationException("BruteForceSerde only supports deserialization");
    }

    @Override
    public void close() {}
}
