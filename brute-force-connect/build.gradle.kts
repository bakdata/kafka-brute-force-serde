description = "Kafka Connect Converter that deserializes messages of an unknown serialization format"

dependencies {
    api(project(":brute-force-core"))

    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.connect.api)
    compileOnly(libs.kafka.connect.runtime)

    implementation(libs.largeMessage.connect)

    testImplementation(platform(libs.kafka.bom))
    testImplementation(libs.kafka.connect.avro.converter) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(libs.kafka.connect.protobuf.converter) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(libs.kafka.connect.json.converter) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }

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

    testImplementation(libs.largeMessage.serde)
    testImplementation(libs.kafka.connect.file)
    testImplementation(libs.kafka.connect.runtime)
    testImplementation(variantOf(libs.kafka.connect.runtime) {
        classifier("test")
    })
    testImplementation(variantOf(libs.kafka.clients) {
        classifier("test")
    })
    testImplementation(libs.kafka.core)
    testImplementation(variantOf(libs.kafka.core) {
        classifier("test")
    })
    testImplementation(variantOf(libs.kafka.server.common) {
        classifier("test")
    })
    testImplementation(libs.kafka.test.common.runtime)
}
