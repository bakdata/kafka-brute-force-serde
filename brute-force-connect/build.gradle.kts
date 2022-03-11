description = "Kafka Connect Converter that deserializes messages of an unknown serialization format"



dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "connect-api", version = kafkaVersion)
    compileOnly(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion)
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-connect-avro-converter", version = confluentVersion)

    val largeMessageVersion: String by project
    implementation(group = "com.bakdata.kafka", name = "large-message-connect", version = largeMessageVersion)

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
    }
    val fluentKafkaVersion = "2.5.1"
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = fluentKafkaVersion
    )

    testImplementation(group = "com.bakdata.kafka", name = "large-message-serde", version = largeMessageVersion)
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
}
