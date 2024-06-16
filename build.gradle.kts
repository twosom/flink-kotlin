import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    kotlin("jvm")
    application
    // shadow
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val flinkVersion = "1.18.1"
setProperty("mainClassName", "NONE")

group = "com.icloud"
version = "1.0-SNAPSHOT"

allprojects {
    plugins.apply {
        apply("java")
        apply("application")
        apply("org.jetbrains.kotlin.jvm")
        apply("com.github.johnrengelman.shadow")
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    repositories {
        mavenCentral()
    }

    kotlin {
        jvmToolchain(8)
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
        implementation ("org.reflections:reflections:0.10.2")  // Reflections 라이브러리 추가


        testImplementation(platform("org.junit:junit-bom:5.10.0"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        implementation(kotlin("stdlib-jdk8"))

        if (project.name != "util") {
            implementation(project(":util"))
        }
    }
}




