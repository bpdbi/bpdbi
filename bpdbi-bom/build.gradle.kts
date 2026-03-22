plugins {
    `java-platform`
    `maven-publish`
    signing
}

// A BOM (Bill of Materials) that aligns versions of all bpdbi modules.
// Users import this as a platform dependency to avoid version conflicts:
//
//   dependencies {
//       implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
//       implementation("io.github.bpdbi:bpdbi-pg-client")    // no version needed
//       implementation("io.github.bpdbi:bpdbi-pool")          // no version needed
//   }

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

publishing {
    publications {
        create<MavenPublication>("mavenBom") {
            from(components["javaPlatform"])

            pom {
                name = "bpdbi-bom"
                description = "Bpdbi Bill of Materials — aligns versions of all Bpdbi modules"
                url = "https://github.com/cies/bpdbi"
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
                    connection = "scm:git:git://github.com/cies/bpdbi.git"
                    developerConnection = "scm:git:ssh://github.com/cies/bpdbi.git"
                    url = "https://github.com/cies/bpdbi"
                }
            }
        }
    }
}

signing {
    val signingKey = providers.environmentVariable("GPG_SIGNING_KEY")
    val signingPassword = providers.environmentVariable("GPG_SIGNING_PASSWORD")
    useInMemoryPgpKeys(signingKey.orNull, signingPassword.orNull)
    sign(publishing.publications["mavenBom"])
    isRequired = !version.toString().endsWith("-SNAPSHOT")
}
