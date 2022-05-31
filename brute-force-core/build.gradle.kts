description = "Base module for Kafka plugins that deserialize messages of an unknown serialization format"

dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
}
