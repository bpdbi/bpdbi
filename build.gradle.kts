plugins {
    `maven-publish`
    signing
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    id("com.diffplug.spotless") version "7.0.4"
    kotlin("jvm") version "2.1.20" apply false
    kotlin("plugin.serialization") version "2.1.20" apply false
}

group = "io.github.bpdbi"
version = "0.1.0-SNAPSHOT"

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl = uri("https://s01.oss.sonatype.org/service/local/")
            snapshotRepositoryUrl =
                uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            username = providers.environmentVariable("OSSRH_USERNAME")
            password = providers.environmentVariable("OSSRH_PASSWORD")
        }
    }
}

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
    "bpdbi-mysql-client",
    "bpdbi-kotlin",
    "bpdbi-record-mapper",
    "bpdbi-javabean-mapper",
    "bpdbi-pool",
)

subprojects {
    if (name in publishedModules) {
        apply(plugin = "maven-publish")
        apply(plugin = "signing")

        // Can't use the `java { }` shorthand here because that accessor is only
        // available when the root project applies the `java` plugin — and we don't,
        // to avoid the root producing an empty JAR. `the<>()` looks up the extension
        // that the subproject's own `java` plugin registered.
        the<JavaPluginExtension>().apply {
            withJavadocJar()
            withSourcesJar()
        }

        publishing {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(components["java"])

                    pom {
                        name = project.name
                        description = "bpdbi — Blocking pipelined SQL client for Java 21+"
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
            sign(publishing.publications["mavenJava"])
            isRequired = !version.toString().endsWith("-SNAPSHOT")
        }
    }
}
