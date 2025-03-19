description = "Kafka Connect Converter that deserializes messages of an unknown serialization format"

dependencies {
    api(project(":brute-force-core"))

    api(group = "org.apache.kafka", name = "connect-api")
    compileOnly(group = "org.apache.kafka", name = "connect-runtime")

    val largeMessageVersion: String by project
    api(platform("com.bakdata.kafka:large-message-bom$largeMessageVersion"))
    implementation(group = "com.bakdata.kafka", name = "large-message-connect")

    testImplementation(group = "io.confluent", name = "kafka-connect-avro-converter")
    testImplementation(group = "io.confluent", name = "kafka-connect-protobuf-converter")
    testImplementation(group = "io.confluent", name = "kafka-connect-json-schema-converter")

    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde")
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde")
    testImplementation(group = "io.confluent", name = "kafka-streams-json-schema-serde")

    val testContainersVersion: String by project
    testImplementation(group = "org.testcontainers", name = "junit-jupiter", version = testContainersVersion)
    testImplementation(group = "org.testcontainers", name = "localstack", version = testContainersVersion)

    testImplementation(group = "com.bakdata.kafka", name = "large-message-serde", version = largeMessageVersion)
    testImplementation(group = "org.apache.kafka", name = "connect-file")
    testImplementation(group = "org.apache.kafka", name = "connect-runtime")
    testImplementation(
        group = "org.apache.kafka",
        name = "connect-runtime",
        classifier = "test"
    )
    testImplementation(group = "org.apache.kafka", name = "kafka-clients", classifier = "test")
    testImplementation(group = "org.apache.kafka", name = "kafka_2.13")
    testImplementation(group = "org.apache.kafka", name = "kafka_2.13", classifier = "test")
    testImplementation(group = "org.apache.kafka", name = "kafka-server-common", classifier = "test")
}
