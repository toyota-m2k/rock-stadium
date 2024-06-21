plugins {
    kotlin("jvm") version "1.9.23"
}

group = "io.github.toyota32k"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.rocksdb:rocksdbjni:9.2.1")
    implementation("org.xerial:sqlite-jdbc:3.46.0.0")
    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.5.5")
    testImplementation("io.kotest:kotest-assertions-core:5.5.5")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}