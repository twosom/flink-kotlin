import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    kotlin("jvm")
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val flinkVersion = "1.17.1"
setProperty("mainClassName", "NONE")


allprojects {
    group = "com.icloud"
    version = "1.0-SNAPSHOT"

    plugins.apply {
        apply("java")
        apply("application")
        apply("org.jetbrains.kotlin.jvm")
        apply("com.github.johnrengelman.shadow")
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    repositories {
        mavenCentral()
    }

    kotlin {
        jvmToolchain(11)
    }

    tasks.test {
        useJUnitPlatform()
    }

    tasks.withType<ShadowJar> {
        isZip64 = true
        mergeServiceFiles()
    }

    dependencies {
        // flink
        implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
        implementation("org.apache.flink:flink-clients:$flinkVersion")
        implementation("org.reflections:reflections:0.10.2")  // Reflections 라이브러리 추가

        // logger
        implementation("org.slf4j:slf4j-jdk14:1.7.32")
        implementation("ch.qos.logback:logback-classic:1.3.1")

        testImplementation(platform("org.junit:junit-bom:5.10.0"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        implementation(kotlin("stdlib-jdk8"))

        if (project.name != "util") {
            implementation(project(":util"))
        }
    }
}




