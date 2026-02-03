description = "Kafka Connect Converter that deserializes messages of an unknown serialization format"

dependencies {
    api(project(":brute-force-core"))

    val kafkaUtilsVersion: String by project
    compileOnly(platform("com.bakdata.kafka:kafka-bom:$kafkaUtilsVersion"))
    compileOnly(group = "org.apache.kafka", name = "connect-api")
    compileOnly(group = "org.apache.kafka", name = "connect-runtime")

    val largeMessageVersion: String by project
    implementation(group = "com.bakdata.kafka", name = "large-message-connect", version = largeMessageVersion)

    testImplementation(platform("com.bakdata.kafka:kafka-bom:$kafkaUtilsVersion"))
    testImplementation(group = "io.confluent", name = "kafka-connect-avro-converter") {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(group = "io.confluent", name = "kafka-connect-protobuf-converter") {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(group = "io.confluent", name = "kafka-connect-json-schema-converter") {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }

    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde") {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde") {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(group = "io.confluent", name = "kafka-streams-json-schema-serde") {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }

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
    testImplementation(group = "org.apache.kafka", name = "kafka-test-common-runtime")
}
