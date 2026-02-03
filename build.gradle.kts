plugins {
    id("com.bakdata.release") version "2.1.0"
    id("com.bakdata.sonar") version "2.1.0"
    id("com.bakdata.sonatype") version "2.1.0"
    id("io.freefair.lombok") version "9.2.0"
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

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(17)
        }
    }

    dependencies {
        val slf4jVersion: String by project
        "implementation"(group = "org.slf4j", name = "slf4j-api", version = slf4jVersion)

        val junitVersion: String by project
        "testRuntimeOnly"(group = "org.junit.platform", name = "junit-platform-launcher", version = junitVersion)
        "testImplementation"(group = "org.junit.jupiter", name = "junit-jupiter", version = junitVersion)
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.27.7")

        val log4jVersion: String by project
        "testImplementation"(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    }

    publication {
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
}
