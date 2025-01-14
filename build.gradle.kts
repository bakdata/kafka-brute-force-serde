plugins {
    id("com.bakdata.release") version "1.4.0"
    id("com.bakdata.sonar") version "1.4.0"
    id("com.bakdata.sonatype") version "1.4.1"
    id("io.freefair.lombok") version "8.4"
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 4
        useJUnitPlatform()
    }

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Salomon Popp")
            id.set("disrupted")
        }
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(11)
        }
    }

    dependencies {
        val slf4jVersion: String by project
        "implementation"(group = "org.slf4j", name = "slf4j-api", version = slf4jVersion)

        val junitVersion: String by project
        "testImplementation"(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
        "testImplementation"(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
        "testRuntimeOnly"(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.27.2")

        val log4jVersion: String by project
        "testImplementation"(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    }
}
