description = "Kafka SerDe that deserializes messages of an unknown serialization format"

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.2.1"
}

dependencies {
    api(project(":brute-force-core"))

    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    val largeMessageVersion: String by project
    implementation(group = "com.bakdata.kafka", name = "large-message-serde", version = largeMessageVersion)
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
    }
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")

    val fluentKafkaVersion = "2.5.1"
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = fluentKafkaVersion
    )
}
