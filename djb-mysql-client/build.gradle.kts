plugins {
    `java-library`
}

dependencies {
    api(project(":djb-core"))

    testImplementation(testFixtures(project(":djb-core")))
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:mysql:1.20.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.1.0")
}

tasks.test {
    useJUnitPlatform()
}
