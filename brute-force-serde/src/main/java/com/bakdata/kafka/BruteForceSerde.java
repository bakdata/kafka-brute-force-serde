package com.bakdata.kafka;

import lombok.experimental.Delegate;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class BruteForceSerde implements Serde<Object> {
    @Delegate
    private final Serde<Object> inner;

    public BruteForceSerde() {
        final Serializer<Object> serializer = new BruteForceSerializer<>();
        final Deserializer<Object> deserializer = new BruteForceDeserializer();
        this.inner = Serdes.serdeFrom(serializer, deserializer);
    }
}
