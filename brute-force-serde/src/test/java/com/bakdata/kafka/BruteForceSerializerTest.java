package com.bakdata.kafka;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

class BruteForceSerializerTest {
    @Test
    void shouldThrowSerializationException() {
        try (final Serializer<String> serializer = new BruteForceSerializer<>()) {
            assertThatExceptionOfType(SerializationException.class)
                    .isThrownBy(() -> serializer.serialize(null, "foo"))
                    .withMessage("BruteForceSerde only supports deserialization");
        }
    }
}
