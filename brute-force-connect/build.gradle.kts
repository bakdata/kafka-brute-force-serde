description = "Kafka Connect Converter that deserializes messages of an unknown serialization format"

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

    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-json-schema-serde", version = confluentVersion)

    val testContainersVersion: String by project
    testImplementation(group = "org.testcontainers", name = "junit-jupiter", version = testContainersVersion)
    testImplementation(group = "org.testcontainers", name = "localstack", version = testContainersVersion)

    testImplementation(group = "com.bakdata.kafka", name = "large-message-serde", version = largeMessageVersion)
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
    testImplementation(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion)
    testImplementation(
        group = "org.apache.kafka",
        name = "connect-runtime",
        version = kafkaVersion,
        classifier = "test"
    )
    testImplementation(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion, classifier = "test")
    testImplementation(group = "org.apache.kafka", name = "kafka_2.13", version = kafkaVersion)
    testImplementation(group = "org.apache.kafka", name = "kafka_2.13", version = kafkaVersion, classifier = "test")
}
