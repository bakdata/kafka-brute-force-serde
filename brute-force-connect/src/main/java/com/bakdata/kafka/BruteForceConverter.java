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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * Kafka {@code Converter} that deserializes messages of an unknown serialization format. Serialization is not supported
 * by this converter.
 * <p>
 *
 * See {@link BruteForceConverterConfig} for the configuration of this converter.
 */
@Slf4j
public class BruteForceConverter implements Converter {

    private List<Converter> converters;
    private boolean shouldIgnoreNoMatch;
    private final Converter fallbackConverter;

    public BruteForceConverter() {
        this.fallbackConverter = new ByteArrayConverter();
    }


    private static List<Converter> createConverters(final Map<String, ?> configs, final boolean isKey,
            final BruteForceConverterConfig bruteForceConfig) {
        final List<Converter> configuredConverter = bruteForceConfig.getConverters();
        configuredConverter.forEach(converter -> converter.configure(configs, isKey));

        if (!bruteForceConfig.isLargeMessageEnabled()) {
            return configuredConverter;
        }

        return configuredConverter.stream()
                .flatMap(converter -> Stream.of(createLargeMessageConverter(configs, isKey, converter), converter))
                .collect(Collectors.toList());
    }

    private static Converter createLargeMessageConverter(final Map<String, ?> configs, final boolean isKey,
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
        this.shouldIgnoreNoMatch = bruteForceConfig.shouldIgnoreNoMatch();
        this.converters = createConverters(configs, isKey, bruteForceConfig);
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

        if (this.shouldIgnoreNoMatch) {
            log.info("No converter matched for topic {}. Falling back to a byte array", topic);
            return this.fallbackConverter.toConnectData(topic, headers, value);
        }

        final String errorMessage =
                String.format("No converter in [%s] was able to deserialize the data", this.converters.stream()
                        .map(converter -> converter.getClass().getName())
                        .collect(Collectors.joining(", ")));
        throw new SerializationException(errorMessage);
    }

}
