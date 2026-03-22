plugins {
    java
    application
}

dependencies {
    implementation(project(":bpdbi-pg-client"))
}

// Not a publishable artifact
tasks.jar { enabled = false }

// Default main class — override with -PmainClass=...
application {
    mainClass = project.findProperty("mainClass") as String?
        ?: "io.github.bpdbi.examples.PgBasicExample"
}
