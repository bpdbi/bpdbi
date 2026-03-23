plugins {
    `java-library`
    `java-test-fixtures`
}

dependencies {
    compileOnly(libs.jspecify)
    testCompileOnly(libs.jspecify)
    testFixturesCompileOnly(libs.jspecify)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testFixturesImplementation(platform(libs.junit.bom))
    testFixturesImplementation(libs.junit.jupiter)
}

tasks.test {
    useJUnitPlatform()
}
