import net.researchgate.release.GitAdapter.GitConfig
import net.researchgate.release.ReleaseExtension

plugins {
    id("net.researchgate.release") version "2.8.1"
    id("com.bakdata.sonar") version "1.1.7"
    id("com.bakdata.sonatype") version "1.1.7"
    id("org.hildan.github.changelog") version "0.8.0"
    id("io.freefair.lombok") version "5.3.3.3"
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
        maven(url = "https://oss.sonatype.org/content/repositories/snapshots")
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

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    githubRepository = "kafka-brute-force-serde"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    dependencies {
        val slf4jVersion: String by project
        "implementation"(group = "org.slf4j", name = "slf4j-api", version = slf4jVersion)

        val junitVersion: String by project
        "testImplementation"(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
        "testImplementation"(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
        "testRuntimeOnly"(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.20.2")

        val log4jVersion: String by project
        "testImplementation"(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    }
}

fun ReleaseExtension.git(configure: GitConfig.() -> Unit) = (getProperty("git") as GitConfig).configure()

release {
    git {
        requireBranch = "main"
    }
}
