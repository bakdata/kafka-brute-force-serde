/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.MessageDefinition;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class BruteForceConverterTest {

    private static final DockerImageName LOCAL_STACK_IMAGE = DockerImageName.parse("localstack/localstack")
            .withTag("1.3.1");
    @Container
    private static final LocalStackContainer LOCAL_STACK_CONTAINER = new LocalStackContainer(LOCAL_STACK_IMAGE)
            .withServices(Service.S3);
    private final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock(List.of(
            new AvroSchemaProvider(),
            new JsonSchemaProvider(),
            new ProtobufSchemaProvider()
    ));

    static S3Client getS3Client() {
        return S3Client.builder()
                .endpointOverride(getEndpointOverride())
                .credentialsProvider(StaticCredentialsProvider.create(getCredentials()))
                .region(getRegion())
                .build();
    }

    private static Region getRegion() {
        return Region.of(LOCAL_STACK_CONTAINER.getRegion());
    }

    private static AwsBasicCredentials getCredentials() {
        return AwsBasicCredentials.create(
                LOCAL_STACK_CONTAINER.getAccessKey(), LOCAL_STACK_CONTAINER.getSecretKey()
        );
    }
    private static final String TOPIC = "topic";

    private static URI getEndpointOverride() {
        return LOCAL_STACK_CONTAINER.getEndpointOverride(Service.S3);
    }

    static Stream<Arguments> generateGenericAvroSerializers() {
        return generateSerializers(new GenericAvroSerde());
    }

    static Stream<Arguments> generateStringSerializers() {
        return generateSerializers(Serdes.String());
    }

    static Stream<Arguments> generateByteArraySerializers() {
        return generateSerializers(Serdes.ByteArray());
    }

    static Stream<Arguments> generateJsonSerializers() {
        return generateSerializers(new KafkaJsonSchemaSerde<>());
    }

    static Stream<Arguments> generateProtobufSerializers() {
        return generateSerializers(new KafkaProtobufSerde<>());
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

    private static DynamicMessage generateDynamicMessage() throws DescriptorValidationException {
        final DynamicSchema dynamicSchema = DynamicSchema.newBuilder()
                .setName("file")
                .addMessageDefinition(MessageDefinition.newBuilder("Test")
                        .addField(null, "string", "testId", 1, null, null)
                        .build())
                .build();
        final Descriptor test = dynamicSchema.getMessageDescriptor("Test");
        return DynamicMessage.newBuilder(test)
                .setField(test.findFieldByName("testId"), "test")
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
        final AwsBasicCredentials credentials = getCredentials();
        return Map.of(
                AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, getEndpointOverride().toString(),
                AbstractLargeMessageConfig.S3_REGION_CONFIG, getRegion().id(),
                AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId(),
                AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey()
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

    @BeforeEach
    void setUp() {
        this.schemaRegistry.start();
    }

    @AfterEach
    void tearDown() {
        this.schemaRegistry.stop();
    }

    @Test
    void shouldIgnoreNoMatch() {
        final byte[] value = {1, 0};
        final Map<String, Object> config = Map.of(BruteForceConverterConfig.CONVERTER_CONFIG, List.of());
        this.testValueConversion(configured(new ByteArraySerializer()), new ByteArraySerializer(), value, config,
                new ByteArrayConverter());
    }

    @Test
    void shouldFailIfIgnoreNoMatchIsDisabled() {
        final byte[] value = {1, 0};

        final SerializerFactory<byte[]> factory = configured(new ByteArraySerializer());
        final ByteArraySerializer expectedSerializer = new ByteArraySerializer();
        final ByteArrayConverter expectedConverter = new ByteArrayConverter();

        final Map<String, Object> config = Map.of(
                BruteForceConverterConfig.CONVERTER_CONFIG, List.of(AvroConverter.class.getName()),
                AbstractBruteForceConfig.IGNORE_NO_MATCH_CONFIG, false
        );

        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(
                        () -> this.testValueConversion(factory, expectedSerializer, value, config, expectedConverter))
                .withMessage(String.format("No converter in [%s, %s] was able to deserialize the data",
                        LargeMessageConverter.class.getName(), AvroConverter.class.getName()));
    }

    @Test
    void shouldFailForLargeMessageSerdeIfDisabled() {
        final GenericRecord value = newGenericRecord();

        final SerializerFactory<GenericRecord> factory = createLargeMessageSerializer(new GenericAvroSerde(), 0, true);
        final GenericAvroSerializer expectedSerializer = new GenericAvroSerializer();
        final AvroConverter expectedConverter = new AvroConverter();

        final Map<String, Object> config = Map.of(
                AbstractBruteForceConfig.LARGE_MESSAGE_ENABLED_CONFIG, false,
                AbstractBruteForceConfig.IGNORE_NO_MATCH_CONFIG, false,
                BruteForceConverterConfig.CONVERTER_CONFIG, List.of(AvroConverter.class.getName())
        );

        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(
                        () -> this.testValueConversion(factory, expectedSerializer, value, config, expectedConverter))
                .withMessage(String.format("No converter in [%s] was able to deserialize the data",
                        AvroConverter.class.getName()));

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

    @ParameterizedTest
    @MethodSource("generateJsonSerializers")
    void shouldConvertJsonKeys(final SerializerFactory<JsonTestRecord> factory) {
        final JsonTestRecord value = new JsonTestRecord("test");
        final Map<String, Object> config = Map.of(
                BruteForceConverterConfig.CONVERTER_CONFIG,
                List.of(AvroConverter.class.getName(), JsonSchemaConverter.class.getName())
        );
        this.testKeyConversion(factory, new KafkaJsonSchemaSerializer<>(), value, config, new JsonSchemaConverter());
    }

    @ParameterizedTest
    @MethodSource("generateJsonSerializers")
    void shouldConvertJsonValues(final SerializerFactory<JsonTestRecord> factory) {
        final JsonTestRecord value = new JsonTestRecord("test");
        final Map<String, Object> config = Map.of(
                BruteForceConverterConfig.CONVERTER_CONFIG,
                List.of(AvroConverter.class.getName(), JsonSchemaConverter.class.getName())
        );
        this.testValueConversion(factory, new KafkaJsonSchemaSerializer<>(), value, config, new JsonSchemaConverter());
    }

    @ParameterizedTest
    @MethodSource("generateProtobufSerializers")
    void shouldConvertProtobufKeys(final SerializerFactory<DynamicMessage> factory)
            throws DescriptorValidationException {
        final DynamicMessage value = generateDynamicMessage();
        final Map<String, Object> config = Map.of(
                BruteForceConverterConfig.CONVERTER_CONFIG,
                List.of(AvroConverter.class.getName(), ProtobufConverter.class.getName())
        );
        this.testKeyConversion(factory, new KafkaProtobufSerializer<>(), value, config, new ProtobufConverter());
    }

    @ParameterizedTest
    @MethodSource("generateProtobufSerializers")
    void shouldConvertProtobufValues(final SerializerFactory<DynamicMessage> factory)
            throws DescriptorValidationException {
        final DynamicMessage value = generateDynamicMessage();
        final Map<String, Object> config = Map.of(
                BruteForceConverterConfig.CONVERTER_CONFIG,
                List.of(AvroConverter.class.getName(), ProtobufConverter.class.getName())
        );
        this.testValueConversion(factory, new KafkaProtobufSerializer<>(), value, config, new ProtobufConverter());
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
        getS3Client().createBucket(CreateBucketRequest.builder()
                .bucket(bucket)
                .build());
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

        final Headers expectedHeaders = new RecordHeaders();
        expectedSerializer.configure(config, isKey);
        final byte[] expectedBytes = expectedSerializer.serialize(TOPIC, expectedHeaders, value);
        expectedConverter.configure(config, isKey);
        final SchemaAndValue expected = expectedConverter.toConnectData(TOPIC, expectedHeaders, expectedBytes);

        assertThat(schemaAndValue.schema()).isEqualTo(expected.schema());
        assertThat(schemaAndValue.value()).isEqualTo(expected.value());
    }

    @FunctionalInterface
    private interface SerializerFactory<T> {

        Serializer<T> create(Map<String, Object> config, boolean isKey);
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JsonTestRecord {
        private String name;
    }
}
