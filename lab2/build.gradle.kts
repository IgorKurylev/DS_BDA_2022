plugins {
    java
    id("application")
    id("io.freefair.lombok") version "6.4.1"
}

group = "com.sixtyten"
version = "1.0-SNAPSHOT"

application {
    mainClass.set("com.sixtyten.hw2.MainApplication")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-core_2.13:3.2.1")
    implementation("org.apache.spark:spark-sql_2.13:3.2.1")
    implementation("org.projectlombok:lombok:1.18.22")
    annotationProcessor("org.projectlombok:lombok:1.18.22")
    implementation("com.googlecode.json-simple:json-simple:1.1.1")

    testImplementation("org.projectlombok:lombok:1.18.22")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.22")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
