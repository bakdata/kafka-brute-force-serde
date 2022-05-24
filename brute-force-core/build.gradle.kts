dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}
