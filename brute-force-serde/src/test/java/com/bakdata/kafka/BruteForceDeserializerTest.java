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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.bakdata.Id;
import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.Test.ProtobufRecord;
import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BruteForceDeserializerTest {
    @RegisterExtension
    static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent()
            .withSecureConnection(false).build();

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private TestTopology<Object, Object> topology = null;

    static Stream<Arguments> generateSpecificAvroSerdes() {
        return generateSerdes(new SpecificAvroSerde<>());
    }

    static Stream<Arguments> generateGenericAvroSerdes() {
        return generateSerdes(new GenericAvroSerde());
    }

    static Stream<Arguments> generateProtobufSerdes() {
        return generateSerdes(new KafkaProtobufSerde<>());
    }

    static Stream<Arguments> generateJsonSerdes() {
        return generateSerdes(new KafkaJsonSchemaSerde<>());
    }

    static Stream<Arguments> generateStringSerdes() {
        return generateSerdes(Serdes.String());
    }

    static Stream<Arguments> generateByteArraySerdes() {
        return generateSerdes(Serdes.ByteArray());
    }

    private static <T> Stream<Arguments> generateSerdes(final Serde<T> baseSerde) {
        return Stream.<Function<Serde<T>, SerdeFactory<T>>>of(
                        BruteForceDeserializerTest::configured,
                        serde -> createLargeMessageSerde(serde, 0, false),
                        serde -> createLargeMessageSerde(serde, Integer.MAX_VALUE, false),
                        serde -> createLargeMessageSerde(serde, 0, true),
                        serde -> createLargeMessageSerde(serde, Integer.MAX_VALUE, true)
                )
                .map(f -> f.apply(baseSerde))
                .map(Arguments::of);
    }

    private static <T> SerdeFactory<T> configured(final Serde<T> s) {
        return (config, isKey) -> {
            s.configure(config, isKey);
            return s;
        };
    }

    private static Properties createProperties(final Properties properties) {
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.putAll(getS3EndpointConfig());
        return properties;
    }

    private static Topology createValueTopology(final Properties properties, final Class<? extends Serde> serdeClass) {
        final StreamsBuilder builder = new StreamsBuilder();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, IntegerSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serdeClass);
        final Serde<Object> serde = new BruteForceSerde();
        serde.configure(new StreamsConfig(properties).originals(), false);
        final KStream<Integer, Object> input = builder.stream(INPUT_TOPIC, Consumed.<Integer, Object>with(null, serde))
                //force usage of default serde. Otherwise, consumed serde would be used for serialization
                .mapValues(self -> self);
        input.to(OUTPUT_TOPIC);
        return builder.build();
    }

    private static Topology createKeyTopology(final Properties properties, final Class<? extends Serde> serdeClass) {
        final StreamsBuilder builder = new StreamsBuilder();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, serdeClass);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, IntegerSerde.class);
        final Serde<Object> serde = new BruteForceSerde();
        serde.configure(new StreamsConfig(properties).originals(), true);
        final KStream<Object, Integer> input = builder.stream(INPUT_TOPIC, Consumed.<Object, Integer>with(serde, null))
                // force usage of default serde. Otherwise, consumed serde would be used for serialization
                .map(KeyValue::new);
        input.to(OUTPUT_TOPIC);
        return builder.build();
    }

    private static <T> SerdeFactory<T> createLargeMessageSerde(final Serde<T> inner, final int maxSize,
            final boolean useHeaders) {
        return (originals, isKey) -> {
            final Serde<T> serde = new LargeMessageSerde<>();
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

    private static GenericRecord newGenericRecord() {
        final Schema schema = SchemaBuilder.record("MyRecord")
                .fields()
                .requiredString("id")
                .endRecord();
        return new GenericRecordBuilder(schema)
                .set("id", "foo")
                .build();
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    @Test
    void shouldReadNullKey() {
        this.createTopology(properties -> createKeyTopology(properties, StringSerde.class), new Properties());
        this.topology.input()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .add(null, 1);
        final List<ProducerRecord<byte[], Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.ByteArray())
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .first()
                .isNull();
    }

    @Test
    void shouldReadNullValue() {
        this.createTopology(properties -> createValueTopology(properties, StringSerde.class), new Properties());
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .add(1, null);
        final List<ProducerRecord<Integer, byte[]>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.ByteArray()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .first()
                .isNull();
    }

    @Test
    void shouldIgnoreNoMatch() {
        final byte[] value = {1, 0};
        final Properties properties = new Properties();
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG, List.of());
        this.testValueTopology(configured(Serdes.ByteArray()), properties, Serdes.ByteArray(), value);
    }

    @Test
    void shouldFailIfIgnoreNoMatchIsDisabled() {
        final byte[] value = {1, 0};
        final Properties properties = new Properties();
        properties.put(AbstractBruteForceConfig.IGNORE_NO_MATCH_CONFIG, false);
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG, List.of(GenericAvroSerde.class.getName()));
        final SerdeFactory<byte[]> serdeFactory = configured(Serdes.ByteArray());
        final Serde<byte[]> serde = Serdes.ByteArray();
        assertThatExceptionOfType(StreamsException.class)
                .isThrownBy(() -> this.testValueTopology(serdeFactory, properties, serde, value))
                .havingCause()
                .isInstanceOf(SerializationException.class)
                .withMessage(String.format("No deserializer in [%s, %s] was able to deserialize the data",
                        LargeMessageDeserializer.class.getName(), GenericAvroDeserializer.class.getName()));
    }

    @Test
    void shouldFailForLargeMessageSerdeIfDisabled() {
        final GenericRecord value = newGenericRecord();
        final SerdeFactory<GenericRecord> factory = createLargeMessageSerde(new GenericAvroSerde(), 0, true);
        final GenericAvroSerde serde = new GenericAvroSerde();

        final Properties properties = new Properties();
        properties.put(AbstractBruteForceConfig.LARGE_MESSAGE_ENABLED_CONFIG, false);
        properties.put(AbstractBruteForceConfig.IGNORE_NO_MATCH_CONFIG, false);
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG, List.of(GenericAvroSerde.class.getName()));

        assertThatExceptionOfType(StreamsException.class)
                .isThrownBy(() -> this.testValueTopology(factory, properties, serde, value))
                .havingCause()
                .isInstanceOf(SerializationException.class)
                .withMessage(String.format("No deserializer in [%s] was able to deserialize the data",
                        GenericAvroDeserializer.class.getName()));
    }

    @ParameterizedTest
    @MethodSource("generateStringSerdes")
    void shouldReadStringValues(final SerdeFactory<String> factory) {
        final String value = "foo";
        this.testValueTopology(factory, new Properties(), Serdes.String(), value);
    }

    @ParameterizedTest
    @MethodSource("generateStringSerdes")
    void shouldReadStringKeys(final SerdeFactory<String> factory) {
        final String value = "foo";
        this.testKeyTopology(factory, new Properties(), Serdes.String(), value);
    }

    @ParameterizedTest
    @MethodSource("generateSpecificAvroSerdes")
    void shouldReadSpecificAvroValues(final SerdeFactory<SpecificRecord> factory) {
        final SpecificRecord value = Id.newBuilder().setId("").build();
        this.testValueTopology(factory, new Properties(), new SpecificAvroSerde<>(), value);
    }

    @ParameterizedTest
    @MethodSource("generateSpecificAvroSerdes")
    void shouldReadSpecificAvroKeys(final SerdeFactory<SpecificRecord> factory) {
        final SpecificRecord value = Id.newBuilder().setId("").build();
        this.testKeyTopology(factory, new Properties(), new SpecificAvroSerde<>(), value);
    }

    @ParameterizedTest
    @MethodSource("generateGenericAvroSerdes")
    void shouldReadGenericAvroValues(final SerdeFactory<GenericRecord> factory) {
        final GenericRecord value = newGenericRecord();
        this.testValueTopology(factory, new Properties(), new GenericAvroSerde(), value);
    }

    @ParameterizedTest
    @MethodSource("generateGenericAvroSerdes")
    void shouldReadGenericAvroKeys(final SerdeFactory<GenericRecord> factory) {
        final GenericRecord value = newGenericRecord();
        this.testKeyTopology(factory, new Properties(), new GenericAvroSerde(), value);
    }

    @ParameterizedTest
    @MethodSource("generateByteArraySerdes")
    void shouldReadBytesValues(final SerdeFactory<byte[]> factory) {
        final Properties properties = new Properties();
        // this makes StringDeserializer fail
        properties.put("value.deserializer.encoding", "missing");

        final byte[] value = {1, 0};
        this.testValueTopology(factory, properties, Serdes.ByteArray(), value);
    }

    @ParameterizedTest
    @MethodSource("generateByteArraySerdes")
    void shouldReadBytesKeys(final SerdeFactory<byte[]> factory) {
        final Properties properties = new Properties();
        // this makes StringDeserializer fail
        properties.put("key.deserializer.encoding", "missing");

        final byte[] value = {1, 0};
        this.testKeyTopology(factory, properties, Serdes.ByteArray(), value);
    }

    @ParameterizedTest
    @MethodSource("generateProtobufSerdes")
    void shouldReadProtobufValues(final SerdeFactory<ProtobufRecord> factory) {
        final ProtobufRecord value = ProtobufRecord.newBuilder().setName("Test").build();
        final Properties properties = new Properties();
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG,
                List.of(GenericAvroSerde.class.getName(), KafkaProtobufSerde.class.getName()));
        this.testValueTopology(factory, properties, new KafkaProtobufSerde<>(ProtobufRecord.class), value);
    }

    @ParameterizedTest
    @MethodSource("generateProtobufSerdes")
    void shouldReadProtobufKeys(final SerdeFactory<ProtobufRecord> factory) {
        final ProtobufRecord value = ProtobufRecord.newBuilder().setName("Test").build();
        final Properties properties = new Properties();
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG,
                List.of(GenericAvroSerde.class.getName(), KafkaProtobufSerde.class.getName()));
        this.testKeyTopology(factory, properties, new KafkaProtobufSerde<>(ProtobufRecord.class), value);
    }

    @ParameterizedTest
    @MethodSource("generateJsonSerdes")
    void shouldReadJsonValues(final SerdeFactory<JsonTestRecord> factory) {
        final JsonTestRecord value = new JsonTestRecord("test");
        final Properties properties = new Properties();
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG,
                List.of(GenericAvroSerde.class.getName(), KafkaJsonSchemaSerde.class.getName()));
        this.testValueTopology(factory, properties, new KafkaJsonSchemaSerde<>(JsonTestRecord.class), value);
    }

    @ParameterizedTest
    @MethodSource("generateJsonSerdes")
    void shouldReadJsonKeys(final SerdeFactory<JsonTestRecord> factory) {
        final JsonTestRecord value = new JsonTestRecord("test");
        final Properties properties = new Properties();
        properties.put(BruteForceSerdeConfig.SERDES_CONFIG,
                List.of(GenericAvroSerde.class.getName(), KafkaJsonSchemaSerde.class.getName()));
        this.testKeyTopology(factory, properties, new KafkaJsonSchemaSerde<>(JsonTestRecord.class), value);
    }

    private <T> void testValueTopology(final SerdeFactory<T> factory, final Properties properties, final Serde<T> serde,
            final T value) {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        this.createTopology(p -> createValueTopology(p, serde.getClass()), properties);

        final Map<String, Object> config = Map.of(
                SCHEMA_REGISTRY_URL_CONFIG, this.topology.getSchemaRegistryUrl(),
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, "s3://" + bucket + "/"
        );
        final Serde<T> inputSerde = factory.create(config, false);
        this.topology.input()
                .withKeySerde(Serdes.Integer())
                .withValueSerde(inputSerde)
                .add(1, value);

        serde.configure(config, false);
        final List<ProducerRecord<Integer, T>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(serde))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(value);
    }

    private <T> void testKeyTopology(final SerdeFactory<T> factory, final Properties properties, final Serde<T> serde,
            final T value) {
        final String bucket = "bucket";
        S3_MOCK.createS3Client().createBucket(bucket);
        this.createTopology(p -> createKeyTopology(p, serde.getClass()), properties);

        final Map<String, Object> config = Map.of(
                SCHEMA_REGISTRY_URL_CONFIG, this.topology.getSchemaRegistryUrl(),
                AbstractLargeMessageConfig.BASE_PATH_CONFIG, "s3://" + bucket + "/"
        );
        final Serde<T> inputSerde = factory.create(config, true);
        this.topology.input()
                .withKeySerde(inputSerde)
                .withValueSerde(Serdes.Integer())
                .add(value, 1);

        serde.configure(config, true);
        final List<ProducerRecord<T, Integer>> records = Seq.seq(this.topology.streamOutput()
                        .withKeySerde(serde)
                        .withValueSerde(Serdes.Integer()))
                .toList();
        assertThat(records)
                .hasSize(1)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder(value);
    }

    private void createTopology(final Function<? super Properties, ? extends Topology> topologyFactory,
            final Properties properties) {
        this.topology = new TestTopology<>(topologyFactory, createProperties(properties))
                .withSchemaRegistryMock(new SchemaRegistryMock(List.of(
                        new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()
                )));
        this.topology.start();
    }

    @FunctionalInterface
    private interface SerdeFactory<T> {

        Serde<T> create(Map<String, Object> config, boolean isKey);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class JsonTestRecord {
        private String name;
    }
}
