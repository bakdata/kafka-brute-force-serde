/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import lombok.experimental.Delegate;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@code Serde} that deserializes messages of an unknown serialization format. Serialization is not supported by
 * this serde.
 * <p>
 * It uses {@link BruteForceSerializer} for serialization and {@link BruteForceDeserializer} for deserialization.
 * <p>
 * Each serialization format that is tested for deserialization is first applied using {@link LargeMessageSerde} and
 * then using the standard serialization format. This serde tests the following format in this order:
 * <ul>
 *     <li>{@link io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde} (if {@code schema.registry.url} is present
 *     in the serde configuration</li>
 *     <li>{@link io.confluent.kafka.streams.serdes.avro.GenericAvroSerde} (if {@code schema.registry.url} is present
 *     in the serde configuration</li>
 *     <li>{@link org.apache.kafka.common.serialization.Serdes.StringSerde}</li>
 *     <li>{@link org.apache.kafka.common.serialization.Serdes.ByteArraySerde}</li>
 * </ul>
 */
public class BruteForceSerde implements Serde<Object> {
    @Delegate
    private final Serde<Object> inner;

    /**
     * Default constructor
     */
    public BruteForceSerde() {
        final Serializer<Object> serializer = new BruteForceSerializer<>();
        final Deserializer<Object> deserializer = new BruteForceDeserializer();
        this.inner = Serdes.serdeFrom(serializer, deserializer);
    }
}
