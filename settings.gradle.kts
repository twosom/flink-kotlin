pluginManagement {
    plugins {
        kotlin("jvm") version "2.0.0"
    }
}
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
rootProject.name = "flink-kotlin"

listOf(
    "util",
    "ch1",
    "ch5",
    "ch6",
    "ch7",
).forEach { include(it) }

