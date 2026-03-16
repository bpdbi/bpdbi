plugins {
    java
}

dependencies {
    implementation(project(":djb-core"))

    testImplementation(testFixtures(project(":djb-core")))
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
