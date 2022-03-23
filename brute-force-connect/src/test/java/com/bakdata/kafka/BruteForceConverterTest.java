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

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.emptyMap;
import static org.apache.kafka.connect.storage.StringConverterConfig.ENCODING_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BruteForceConverterTest {
    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().withSecureConnection(false).build();
    private static final String TOPIC = "topic";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistry = new SchemaRegistryMockExtension();

    static Stream<Arguments> generateGenericAvroSerializers() {
        return generateSerializers(new GenericAvroSerde());
    }

    static Stream<Arguments> generateStringSerializers() {
        return generateSerializers(Serdes.String());
    }

    static Stream<Arguments> generateByteArraySerializers() {
        return generateSerializers(Serdes.ByteArray());
    }

    static GenericRecord newGenericRecord() {
        final Schema schema = SchemaBuilder.record("MyRecord")
                .fields()
                .requiredString("id")
                .endRecord();
        return new GenericRecordBuilder(schema)
                .set("id", "foo")
                .build();
    }

    private static SchemaAndValue toConnectData(final String text) {
        return new StringConverter().toConnectData(null, text.getBytes());
    }

    private static <T> SerializerFactory<T> createLargeMessageSerializer(final Serde<T> inner, final int maxSize,
            final boolean useHeaders) {
        return (originals, isKey) -> {
            final Serializer<T> serde = new LargeMessageSerializer<>();
            final Map<String, Object> configs = new HashMap<>(originals);
            configs.putAll(getS3EndpointConfig());
            configs.put(isKey ? LargeMessageSerdeConfig.KEY_SERDE_CLASS_CONFIG
                    : LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, inner.getClass());
            configs.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, maxSize);
            configs.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, useHeaders);
            serde.configure(configs, isKey);
            return serde;
        };
    }

    private static Map<String, Object> getS3EndpointConfig() {
        return Map.of(
                AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, "http://localhost:" + S3_MOCK.getHttpPort(),
                AbstractLargeMessageConfig.S3_REGION_CONFIG, "us-east-1",
                AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, "foo",
                AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, "bar",
                AbstractLargeMessageConfig.S3_ENABLE_PATH_STYLE_ACCESS_CONFIG, true
        );
    }

    private static <T> SerializerFactory<T> configured(final Serializer<T> serializer) {
        return (config, isKey) -> {
            serializer.configure(config, isKey);
            return serializer;
        };
    }

    private static <T> Stream<Arguments> generateSerializers(final Serde<T> baseSerde) {
        return Stream.<Function<Serde<T>, SerializerFactory<T>>>of(
                        serde -> configured(serde.serializer()),
                        serde -> createLargeMessageSerializer(serde, 0, false),
                        serde -> createLargeMessageSerializer(serde, Integer.MAX_VALUE, false),
                        serde -> createLargeMessageSerializer(serde, 0, true),
                        serde -> createLargeMessageSerializer(serde, Integer.MAX_VALUE, true)
                )
                .map(f -> f.apply(baseSerde))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("generateStringSerializers")
    void shouldConvertStringValues(final SerializerFactory<String> factory) {
        final String value = "test";
        this.testValueConversion(factory, new StringSerializer(), value, emptyMap(), new StringConverter());
    }

    @ParameterizedTest
    @MethodSource("generateStringSerializers")
    void shouldConvertStringKeys(final SerializerFactory<String> factory) {
        final String value = "test";
        this.testKeyConversion(factory, new StringSerializer(), value, emptyMap(), new StringConverter());
    }

    @ParameterizedTest
    @MethodSource("generateGenericAvroSerializers")
    void shouldConvertAvroValues(final SerializerFactory<GenericRecord> factory) {
        final GenericRecord value = newGenericRecord();
        this.testValueConversion(factory, new GenericAvroSerializer(), value, emptyMap(), new AvroConverter());
    }

    @ParameterizedTest
    @MethodSource("generateGenericAvroSerializers")
    void shouldConvertAvroKeys(final SerializerFactory<GenericRecord> factory) {
        final GenericRecord value = newGenericRecord();
        this.testKeyConversion(factory, new GenericAvroSerializer(), value, emptyMap(), new AvroConverter());
    }

    @ParameterizedTest
    @MethodSource("generateByteArraySerializers")
    void shouldConvertByteValues(final SerializerFactory<byte[]> factory) {
        final byte[] value = {1, 0};
        final Map<String, Object> config = Map.of(ENCODING_CONFIG, "missing");
        this.testValueConversion(factory, new ByteArraySerializer(), value, config, new ByteArrayConverter());
    }

    @ParameterizedTest
    @MethodSource("generateByteArraySerializers")
    void shouldConvertByteKeys(final SerializerFactory<byte[]> factory) {
        final byte[] value = {1, 0};
        final Map<String, Object> config = Map.of(ENCODING_CONFIG, "missing");
        this.testKeyConversion(factory, new ByteArraySerializer(), value, config, new ByteArrayConverter());
    }

    @Test
    void shouldThrowSerializationException() {
        final SchemaAndValue data = toConnectData("test");
        final Converter converter = new BruteForceConverter();
        final org.apache.kafka.connect.data.Schema schema = data.schema();
        final Object value = data.value();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> converter.fromConnectData(TOPIC, schema, value))
                .withMessage("BruteForceConverter only supports converting to connect data");
    }

    private <T> void testValueConversion(final SerializerFactory<T> factory,
            final Serializer<? super T> expectedSerializer, final T value, final Map<String, Object> originals,
            final Converter expectedConverter) {
        this.testConversion(factory, expectedSerializer, value, originals, expectedConverter, false);
    }

    private <T> void testKeyConversion(final SerializerFactory<T> factory,
            final Serializer<? super T> expectedSerializer, final T value, final Map<String, Object> originals,
            final Converter expectedConverter) {
        this.testConversion(factory, expectedSerializer, value, originals, expectedConverter, true);
    }

    private <T> void testConversion(final SerializerFactory<T> factory, final Serializer<? super T> expectedSerializer,
            final T value, final Map<String, Object> originals, final Converter expectedConverter,
            final boolean isKey) {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        final Map<String, Object> config = new HashMap<>(originals);
        config.put(SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistry.getUrl());
        config.put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, "s3://" + bucket + "/");
        config.putAll(getS3EndpointConfig());

        final Serializer<T> serializer = factory.create(config, isKey);
        final Headers headers = new RecordHeaders();
        final byte[] bytes = serializer.serialize(TOPIC, headers, value);
        final Converter converter = new BruteForceConverter();
        converter.configure(config, isKey);
        final SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, headers, bytes);

        expectedSerializer.configure(config, isKey);
        final byte[] expectedBytes = expectedSerializer.serialize(TOPIC, headers, value);
        expectedConverter.configure(config, isKey);
        final SchemaAndValue expected = expectedConverter.toConnectData(TOPIC, headers, expectedBytes);

        assertThat(schemaAndValue.schema()).isEqualTo(expected.schema());
        assertThat(schemaAndValue.value()).isEqualTo(expected.value());
    }

    @FunctionalInterface
    private interface SerializerFactory<T> {

        Serializer<T> create(Map<String, Object> config, boolean isKey);
    }
}
