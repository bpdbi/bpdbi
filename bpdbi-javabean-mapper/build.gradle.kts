plugins {
  `java-library`
}

dependencies {
  api(project(":bpdbi-core"))
  compileOnly(libs.jspecify)

  testImplementation(testFixtures(project(":bpdbi-core")))
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
  useJUnitPlatform()
}
