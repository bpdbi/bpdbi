plugins {
  `java-platform`
  id("com.vanniktech.maven.publish")
}

// A BOM (Bill of Materials) that aligns versions of all bpdbi modules.
// Users import this as a platform dependency to avoid version conflicts:
//
//     dependencies {
//       implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
//       implementation("io.github.bpdbi:bpdbi-pg-client") // no version needed
//       implementation("io.github.bpdbi:bpdbi-pool") // no version needed
//     }

dependencies {
  constraints {
    api(project(":bpdbi-core"))
    api(project(":bpdbi-pg-client"))
    api(project(":bpdbi-kotlin"))
    api(project(":bpdbi-record-mapper"))
    api(project(":bpdbi-javabean-mapper"))
    api(project(":bpdbi-pool"))
  }
}

mavenPublishing {
  configure(com.vanniktech.maven.publish.JavaPlatform())
  publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
  signAllPublications()

  pom {
    name = "bpdbi-bom"
    description = "Bpdbi BOM (bill of materials) — aligns versions of all bpdbi modules"
    url = "https://github.com/bpdbi/bpdbi"
    licenses {
      license {
        name = "The Apache License, Version 2.0"
        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
      }
    }
    developers {
      developer {
        id = "cies"
        name = "Cies Breijs"
        url = "https://github.com/cies"
      }
    }
    scm {
      connection = "scm:git:git://github.com/bpdbi/bpdbi.git"
      developerConnection = "scm:git:ssh://github.com/bpdbi/bpdbi.git"
      url = "https://github.com/bpdbi/bpdbi"
    }
  }
}

// Signing is required for remote publishes but not for local
gradle.taskGraph.whenReady {
  tasks.withType<Sign>().configureEach {
    onlyIf { !gradle.taskGraph.hasTask(":bpdbi-bom:publishToMavenLocal") }
  }
}
