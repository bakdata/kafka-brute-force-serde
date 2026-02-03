description = "Kafka SerDe that deserializes messages of an unknown serialization format"

plugins {
    alias(libs.plugins.avro)
    alias(libs.plugins.protobuf)
}

dependencies {
    api(project(":brute-force-core"))

    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.clients)
    implementation(libs.largeMessage.serde)

    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(libs.kafka.streams.protobuf.serde) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(libs.kafka.streams.json.serde) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }

    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.localstack)

    testImplementation(libs.fluentKafkaStreamsTests)
}

val protobufVersion = libs.protobuf.get().version
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}
