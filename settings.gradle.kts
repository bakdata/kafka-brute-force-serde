pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }
}

rootProject.name = "brute-force"

include(":brute-force-core", ":brute-force-serde", ":brute-force-connect")
