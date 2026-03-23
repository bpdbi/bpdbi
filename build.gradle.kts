plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.vanniktech.maven.publish) apply false
    alias(libs.plugins.spotless)
}

group = "io.github.bpdbi"
version = "0.1.0"

subprojects {
    group = rootProject.group
    version = rootProject.version

    repositories {
        mavenCentral()
    }

    if (name != "bpdbi-bom") {
        apply(plugin = "java")
        apply(plugin = "jacoco")
        apply(plugin = "com.diffplug.spotless")

        configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(21)
            }
        }

        tasks.withType<JavaCompile> {
            options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
        }

        configure<com.diffplug.gradle.spotless.SpotlessExtension> {
            java {
                googleJavaFormat()
            }
        }

        tasks.withType<JacocoReport> {
            reports {
                xml.required = true
                html.required = true
            }
        }
    }
}

// Publishing config for all library modules (excludes examples)
val publishedModules = setOf(
    "bpdbi-core",
    "bpdbi-pg-client",
    "bpdbi-kotlin",
    "bpdbi-record-mapper",
    "bpdbi-javabean-mapper",
    "bpdbi-pool",
)

subprojects {
    if (name in publishedModules) {
        apply(plugin = "com.vanniktech.maven.publish")

        configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
            publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
            signAllPublications()

            pom {
                name = project.name
                description = "Blocking pipelined SQL client for Postgres"
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
                onlyIf { !gradle.taskGraph.hasTask(":${project.name}:publishToMavenLocal") }
            }
        }
    }
}
