description = "Base module for Kafka plugins that deserialize messages of an unknown serialization format"

dependencies {
    api(platform("com.bakdata.kafka:kafka-bom:1.1.0"))
    api(group = "org.apache.kafka", name = "kafka-clients")
}
