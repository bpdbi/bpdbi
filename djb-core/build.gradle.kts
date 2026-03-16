plugins {
    `java-library`
    `java-test-fixtures`
}

dependencies {
    api("org.jspecify:jspecify:1.0.0")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testFixturesImplementation(platform("org.junit:junit-bom:5.11.4"))
    testFixturesImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}
