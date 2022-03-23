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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

/**
 * Kafka {@code Deserializer} that deserializes messages of an unknown serialization format.
 * <p>
 * Each serialization format that is tested for deserialization is first applied using {@link LargeMessageDeserializer}
 * and then using the standard serialization format. This serde tests the following format in this order:
 * <ul>
 *     <li>{@link io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer} (if {@code schema.registry.url} is
 *     present in the serde configuration</li>
 *     <li>{@link io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer} (if {@code schema.registry.url} is
 *     present in the serde configuration</li>
 *     <li>{@link org.apache.kafka.common.serialization.StringDeserializer}</li>
 *     <li>{@link ByteArrayDeserializer}</li>
 * </ul>
 */
@NoArgsConstructor
@Slf4j
public class BruteForceDeserializer implements Deserializer<Object> {

    private static final List<Supplier<Serde<?>>> DEFAULT_FACTORIES = List.of(
            StringSerde::new,
            ByteArraySerde::new
    );
    private List<Deserializer<Object>> deserializers;

    private static Map<String, Object> createLargeMessageConfig(final Map<String, ?> configs, final boolean isKey,
            final Serde<Object> serde) {
        final Map<String, Object> conf = new HashMap<>(configs);
        conf.put(isKey ? LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG
                        : LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG,
                serde.getClass());
        return conf;
    }

    private static Stream<Deserializer<Object>> createDeserializers(final Map<String, ?> configs, final boolean isKey,
            final Supplier<Serde<?>> factory) {
        final Serde<Object> serde = (Serde<Object>) factory.get();
        final Deserializer<Object> deserializer = serde.deserializer();
        deserializer.configure(configs, isKey);
        final Deserializer<Object> largeMessageDeserializer = new LargeMessageDeserializer<>();
        final Map<String, Object> largeMessageConfigs = createLargeMessageConfig(configs, isKey, serde);
        largeMessageDeserializer.configure(largeMessageConfigs, isKey);
        return Stream.of(largeMessageDeserializer, deserializer);
    }

    private static Stream<Supplier<Serde<?>>> getFactories(final Map<String, ?> configs) {
        final Collection<Supplier<Serde<?>>> factories = new ArrayList<>();
        if (configs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
            factories.add(SpecificAvroSerde::new);
            factories.add(GenericAvroSerde::new);
        }
        factories.addAll(DEFAULT_FACTORIES);
        return factories.stream();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        this.deserializers = getFactories(configs)
                .flatMap(factory -> createDeserializers(configs, isKey, factory))
                .collect(Collectors.toList());
    }

    /**
     * @since 1.1.0
     * @deprecated Use {@link Deserializer#deserialize(String, Headers, byte[])}
     */
    @Override
    @Deprecated(since = "1.1.0")
    public Object deserialize(final String topic, final byte[] data) {
        return this.deserialize(topic, new RecordHeaders(), data);
    }

    @Override
    public Object deserialize(final String topic, final Headers headers, final byte[] data) {
        Objects.requireNonNull(this.deserializers, "You need to configure the deserializer first");
        for (final Deserializer<Object> deserializer : this.deserializers) {
            final Class<? extends Deserializer> clazz = deserializer.getClass();
            try {
                final Object value = deserializer.deserialize(topic, headers, data);
                log.trace("Deserialized message using {}", clazz);
                return value;
            } catch (final RuntimeException ex) {
                log.trace("Failed deserializing message using {}", clazz, ex);
            }
        }
        throw new IllegalStateException("Deserialization should have worked with " + ByteArrayDeserializer.class);
    }

    @Override
    public void close() {
        this.deserializers.forEach(Deserializer::close);
    }
}
