plugins {
    `java-library`
}

dependencies {
    api(project(":djb-core"))
    implementation("com.ongres.scram:scram-client:3.1")

    testImplementation(testFixtures(project(":djb-core")))
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
