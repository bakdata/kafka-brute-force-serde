import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

description = "Kafka SerDe that deserializes messages of an unknown serialization format"

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("com.google.protobuf") version "0.8.18"
}

repositories {
    // required for kafka-streams-json-schema-serde dependency
    maven(url = "https://jitpack.io")
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

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.4.16") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
    }
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")

    val fluentKafkaVersion = "2.11.3"
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = fluentKafkaVersion
    )

    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = fluentKafkaVersion
    )
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.18.1"
    }
}

sourceSets {
    test {
        java.srcDirs("build/generated/source/proto/test/java")
    }
}
