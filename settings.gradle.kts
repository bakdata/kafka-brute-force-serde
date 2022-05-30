pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

rootProject.name = "brute-force"

include(":brute-force-core", ":brute-force-serde", ":brute-force-connect")
