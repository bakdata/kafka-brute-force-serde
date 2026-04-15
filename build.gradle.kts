plugins {
    alias(libs.plugins.release)
    alias(libs.plugins.sonar)
    alias(libs.plugins.sonatype)
    alias(libs.plugins.lombok)
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 4
        useJUnitPlatform()
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
        "implementation"(rootProject.libs.slf4j.api)

        "testRuntimeOnly"(rootProject.libs.junit.platform.launcher)
        "testImplementation"(rootProject.libs.junit.jupiter)
        "testImplementation"(rootProject.libs.assertj)

        "testImplementation"(rootProject.libs.log4j.slf4j2)
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
