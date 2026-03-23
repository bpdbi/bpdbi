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

// Test fixtures are for internal use only — exclude them from publication
val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
afterEvaluate {
    configurations.findByName("testFixturesSourcesElements")?.let {
        javaComponent.withVariantsFromConfiguration(it) { skip() }
    }
}
