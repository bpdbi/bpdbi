plugins {
    java
    application
}

dependencies {
    implementation(project(":djb-pg-client"))
    implementation(project(":djb-mysql-client"))
}

// Not a publishable artifact
tasks.jar { enabled = false }

// Default main class — override with -PmainClass=...
application {
    mainClass = project.findProperty("mainClass") as String?
        ?: "io.djb.examples.PgBasicExample"
}
