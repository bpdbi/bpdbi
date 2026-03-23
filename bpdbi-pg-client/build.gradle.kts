plugins {
  `java-library`
}

dependencies {
  api(project(":bpdbi-core"))
  compileOnly(libs.jspecify)
  implementation(libs.scram.client)

  testImplementation(testFixtures(project(":bpdbi-core")))
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
  useJUnitPlatform()
}
