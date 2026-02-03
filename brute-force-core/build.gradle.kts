description = "Base module for Kafka plugins that deserialize messages of an unknown serialization format"

dependencies {
    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.clients)

    testImplementation(platform(libs.kafka.bom))
    testImplementation(libs.kafka.clients)
}
