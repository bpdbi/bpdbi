plugins {
    `java-platform`
    `maven-publish`
    signing
}

// A BOM (Bill of Materials) that aligns versions of all djb modules.
// Users import this as a platform dependency to avoid version conflicts:
//
//   dependencies {
//       implementation(platform("io.djb:djb-bom:0.1.0"))
//       implementation("io.djb:djb-pg-client")    // no version needed
//       implementation("io.djb:djb-pool")          // no version needed
//   }

dependencies {
    constraints {
        api(project(":djb-core"))
        api(project(":djb-pg-client"))
        api(project(":djb-mysql-client"))
        api(project(":djb-kotlin"))
        api(project(":djb-record-mapper"))
        api(project(":djb-javabean-mapper"))
        api(project(":djb-pool"))
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenBom") {
            from(components["javaPlatform"])

            pom {
                name = "djb-bom"
                description = "djb Bill of Materials — aligns versions of all djb modules"
                url = "https://github.com/cies/djb"
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
                    connection = "scm:git:git://github.com/cies/djb.git"
                    developerConnection = "scm:git:ssh://github.com/cies/djb.git"
                    url = "https://github.com/cies/djb"
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
