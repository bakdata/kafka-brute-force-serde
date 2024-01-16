description = "Kafka Connect Converter that deserializes messages of an unknown serialization format"

repositories {
    // required for kafka-streams-json-schema-serde dependency
    maven(url = "https://jitpack.io")
}

dependencies {
    api(project(":brute-force-core"))

    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "connect-api", version = kafkaVersion)
    compileOnly(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion)

    val largeMessageVersion: String by project
    implementation(group = "com.bakdata.kafka", name = "large-message-connect", version = largeMessageVersion)

    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-connect-avro-converter", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-connect-protobuf-converter", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-connect-json-schema-converter", version = confluentVersion)

    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-json-schema-serde", version = confluentVersion)

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.4.16") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
    }
    val fluentKafkaVersion = "2.11.3"
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = fluentKafkaVersion
    )

    testImplementation(group = "com.bakdata.kafka", name = "large-message-serde", version = largeMessageVersion)
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.5.0") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
}
