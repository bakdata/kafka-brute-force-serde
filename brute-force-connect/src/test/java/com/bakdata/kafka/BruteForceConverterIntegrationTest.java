/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static com.bakdata.kafka.BruteForceConverterTest.newGenericRecord;
import static org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode.HYBRID_WARN;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
class BruteForceConverterIntegrationTest {

    private static final DockerImageName LOCAL_STACK_IMAGE = DockerImageName.parse("localstack/localstack")
            .withTag("1.3.1");
    @Container
    private static final LocalStackContainer LOCAL_STACK_CONTAINER = new LocalStackContainer(LOCAL_STACK_IMAGE)
            .withServices(Service.S3);
    private static final String SCHEMA_REGISTRY_URL = "mock://";
    private static final String BUCKET_NAME = "testbucket";
    private static final String TOPIC = "input";
    private EmbeddedConnectCluster kafkaCluster;
    @TempDir
    private File outputDir;

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

    private static URI getEndpointOverride() {
        return LOCAL_STACK_CONTAINER.getEndpointOverride(Service.S3);
    }

    private static Map<String, String> createS3BackedProperties() {
        final Map<String, String> properties = new HashMap<>();
        final AwsBasicCredentials credentials = getCredentials();
        properties.put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG,
                getEndpointOverride().toString());
        properties.put(AbstractLargeMessageConfig.S3_REGION_CONFIG, getRegion().id());
        properties.put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId());
        properties.put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey());
        properties.put(AbstractLargeMessageConfig.BASE_PATH_CONFIG, String.format("s3://%s/", BUCKET_NAME));
        return properties;
    }

    private static String withValuePrefix(final Object config) {
        return ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + config;
    }

    private static Map<String, String> config(final File file) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, FileStreamSinkConnector.class.getName());
        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        properties.put(FileStreamSinkConnector.FILE_CONFIG, file.getAbsolutePath());
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, BruteForceConverter.class.getName());
        properties.put(withValuePrefix(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),
                SCHEMA_REGISTRY_URL);
        createS3BackedProperties().forEach((key, value) -> properties.put(withValuePrefix(key), value));
        return properties;
    }

    private static Map<String, Object> createBackedStringProducerProperties(final boolean shouldBack) {
        final Map<String, Object> properties = createBaseProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LargeMessageSerializer.class);
        properties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        properties.put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, shouldBack ? 0 : Integer.MAX_VALUE);
        properties.putAll(createS3BackedProperties());
        return properties;
    }

    private static Map<String, Object> createBaseProducerProperties() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private static Map<String, Object> createStringProducerProperties() {
        final Map<String, Object> properties = createBaseProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private static Map<String, Object> createAvroProducerProperties() {
        final Map<String, Object> properties = createBaseProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        return properties;
    }

    @BeforeEach
    void setUp() {
        getS3Client().createBucket(CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build());
        this.kafkaCluster = new EmbeddedConnectCluster.Builder()
                .name("test-cluster")
                .workerProps(new HashMap<>(Map.of( // map needs to be mutable
                        // FIXME make compatible with service discovery
                        WorkerConfig.PLUGIN_DISCOVERY_CONFIG, HYBRID_WARN.toString()
                )))
                .build();
        this.kafkaCluster.start();
    }

    @AfterEach
    void tearDown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldProcessRecordsCorrectly() throws InterruptedException, IOException {
        this.kafkaCluster.kafka().createTopic(TOPIC);
        final File file = new File(this.outputDir, "out");
        this.kafkaCluster.configureConnector("test", config(file));

        try (final Producer<String, String> producer = this.createProducer(
                createBackedStringProducerProperties(true))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "toS3"));
        }

        try (final Producer<String, String> producer = this.createProducer(
                createBackedStringProducerProperties(false))) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "local"));
        }

        try (final Producer<String, String> producer = this.createProducer(createStringProducerProperties())) {
            producer.send(new ProducerRecord<>(TOPIC, "key", "regular"));
        }

        try (final Producer<String, GenericRecord> producer = this.createProducer(createAvroProducerProperties())) {
            producer.send(new ProducerRecord<>(TOPIC, "key", newGenericRecord()));
        }

        // makes sure that all records are processed
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        final List<String> output = Files.readAllLines(file.toPath());
        assertThat(output).containsExactly("toS3", "local", "regular", "Struct{id=foo}");
    }

    @SuppressWarnings("unchecked") // Producer always uses byte[] although serializer is customizable
    private <K, V> Producer<K, V> createProducer(final Map<String, Object> properties) {
        return (Producer<K, V>) this.kafkaCluster.kafka()
                .createProducer(properties);
    }
}

