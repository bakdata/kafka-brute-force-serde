description = "Base module for Kafka plugins that deserialize messages of an unknown serialization format"

dependencies {
    val kafkaUtilsVersion: String by project
    compileOnly(platform("com.bakdata.kafka:kafka-bom:$kafkaUtilsVersion"))
    compileOnly(group = "org.apache.kafka", name = "kafka-clients")

    testImplementation(platform("com.bakdata.kafka:kafka-bom:$kafkaUtilsVersion"))
    testImplementation(group = "org.apache.kafka", name = "kafka-clients")
}
