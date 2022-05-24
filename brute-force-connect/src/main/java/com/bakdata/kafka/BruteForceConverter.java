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

import io.confluent.connect.avro.AvroConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

/**
 * Kafka {@code Converter} that deserializes messages of an unknown serialization format. Serialization is not supported
 * by this converter.
 * <p>
 * Each serialization format that is tested for deserialization is first applied using {@link LargeMessageConverter} and
 * then using the standard serialization format. This converter tests the following format in this order:
 * <ul>
 *     <li>{@link AvroConverter} (if {@code schema.registry.url} is present in the converter configuration</li>
 *     <li>{@link StringConverter}</li>
 *     <li>{@link ByteArrayConverter}</li>
 * </ul>
 */
@NoArgsConstructor
@Slf4j
public class BruteForceConverter implements Converter {

    private List<Converter> converters;

    private static Converter createLargeMessageConverters(final Map<String, ?> configs, final boolean isKey,
            final Converter converter) {
        final Converter largeMessageConverter = new LargeMessageConverter();
        final Map<String, Object> largeMessageConfigs = createLargeMessageConfig(configs, converter);
        largeMessageConverter.configure(largeMessageConfigs, isKey);
        return largeMessageConverter;
    }

    private static Map<String, Object> createLargeMessageConfig(final Map<String, ?> configs,
            final Converter converter) {
        final Map<String, Object> conf = new HashMap<>(configs);
        conf.put(LargeMessageConverterConfig.CONVERTER_CLASS_CONFIG, converter.getClass());
        return conf;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final BruteForceConverterConfig bruteForceConfig = new BruteForceConverterConfig(configs);
        Stream<Converter> converterStream = bruteForceConfig.getConverters().stream()
                .peek(converter -> converter.configure(configs, isKey));

        if (bruteForceConfig.isLargeMessageEnabled()) {
            converterStream = converterStream.flatMap(converter -> Stream.of(
                    createLargeMessageConverters(configs, isKey, converter),
                    converter
            ));
        }

        this.converters = converterStream.collect(Collectors.toList());
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        throw new SerializationException(
                BruteForceConverter.class.getSimpleName() + " only supports converting to connect data");
    }

    /**
     * @since 1.1.0
     * @deprecated Use {@link Converter#toConnectData(String, Headers, byte[])}
     */
    @Override
    @Deprecated(since = "1.1.0")
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        return this.toConnectData(topic, new RecordHeaders(), value);
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final Headers headers, final byte[] value) {
        Objects.requireNonNull(this.converters, "You need to configure the converter first");
        for (final Converter converter : this.converters) {
            final Class<? extends Converter> clazz = converter.getClass();
            try {
                final SchemaAndValue schemaAndValue = converter.toConnectData(topic, headers, value);
                log.trace("Converted message using {}", clazz);
                return schemaAndValue;
            } catch (final RuntimeException ex) {
                log.trace("Failed converting message using {}", clazz, ex);
            }
        }
        throw new IllegalStateException("Conversion should have worked with " + ByteArrayConverter.class);
    }
}
