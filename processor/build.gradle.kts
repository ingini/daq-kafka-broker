plugins {
    kotlin("jvm") version "2.0.0"
    application
}

repositories { mavenCentral() }

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.7.0")
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.1")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("software.amazon.awssdk:s3:2.26.12")
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.prometheus:simpleclient_httpserver:0.16.0")
    implementation("io.prometheus:simpleclient_hotspot:0.16.0")
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass = "com.redpoc.processor.ApplicationKt"
}

kotlin { jvmToolchain(21) }

tasks.withType<Jar> {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest { attributes["Main-Class"] = "com.redpoc.processor.ApplicationKt" }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
