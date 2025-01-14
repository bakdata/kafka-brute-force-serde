description = "Kafka SerDe that deserializes messages of an unknown serialization format"

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("com.google.protobuf") version "0.9.4"
}

dependencies {
    api(project(":brute-force-core"))

    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    val largeMessageVersion: String by project
    implementation(group = "com.bakdata.kafka", name = "large-message-serde", version = largeMessageVersion)

    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-json-schema-serde", version = confluentVersion)

    val testContainersVersion: String by project
    testImplementation(group = "org.testcontainers", name = "junit-jupiter", version = testContainersVersion)
    testImplementation(group = "org.testcontainers", name = "localstack", version = testContainersVersion)
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.15")

    val fluentKafkaVersion = "3.0.0"
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = fluentKafkaVersion
    )
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.5"
    }
}
