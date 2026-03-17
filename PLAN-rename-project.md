# Plan: Rename project from Djb to Bpdbi

Rename all occurrences of `djb`/`Djb` to `bpdbi`/`Bpdbi` across the entire project.

## Naming conventions

| Context               | Old                | New                        |
|-----------------------|--------------------|----------------------------|
| English text          | Djb                | Bpdbi                      |
| Package namespace     | `io.djb`           | `io.github.bpdbi`          |
| Maven group           | `io.djb`           | `io.github.bpdbi`          |
| Module/artifact names | `djb-core`         | `bpdbi-core`               |
| Root project name     | `djb`              | `bpdbi`                    |
| GitHub repo           | `cies/djb`         | `cies/bpdbi`               |
| Repo directory        | `Repos/djb`        | `Repos/bpdbi`              |

**Package naming rule:** package names mirror the subproject names. For example,
subproject `bpdbi-core` has root package `io.github.bpdbi.core`, subproject `bpdbi-pg-client`
has root package `io.github.bpdbi.pg` (same as the current `io.djb.pg`), etc.

### Full package mapping

| Subproject             | Old package              | New package                          |
|------------------------|--------------------------|--------------------------------------|
| `bpdbi-core`           | `io.djb`                 | `io.github.bpdbi.core`               |
| (core impl)            | `io.djb.impl`            | `io.github.bpdbi.core.impl`          |
| (core test fixtures)   | `io.djb.test`            | `io.github.bpdbi.core.test`          |
| `bpdbi-pg-client`      | `io.djb.pg`              | `io.github.bpdbi.pg`                 |
| (pg data)              | `io.djb.pg.data`         | `io.github.bpdbi.pg.data`            |
| (pg impl auth)         | `io.djb.pg.impl.auth`    | `io.github.bpdbi.pg.impl.auth`       |
| (pg impl codec)        | `io.djb.pg.impl.codec`   | `io.github.bpdbi.pg.impl.codec`      |
| `bpdbi-mysql-client`   | `io.djb.mysql`           | `io.github.bpdbi.mysql`              |
| (mysql impl auth)      | `io.djb.mysql.impl.auth` | `io.github.bpdbi.mysql.impl.auth`    |
| (mysql impl codec)     | `io.djb.mysql.impl.codec`| `io.github.bpdbi.mysql.impl.codec`   |
| `bpdbi-kotlin`         | `io.djb.kotlin`          | `io.github.bpdbi.kotlin`             |
| `bpdbi-record-mapper`  | `io.djb.mapper`          | `io.github.bpdbi.mapper`             |
| `bpdbi-javabean-mapper`| `io.djb.mapper`          | `io.github.bpdbi.mapper`             |
| `bpdbi-pool`           | `io.djb.pool`            | `io.github.bpdbi.pool`               |
| `examples`             | `io.djb.examples`        | `io.github.bpdbi.examples`           |

Note: the core module's current root package `io.djb` (with no subpackage) becomes
`io.github.bpdbi.core`. This means all core classes (`Connection`, `Row`, `RowSet`, etc.)
move from `io.djb` to `io.github.bpdbi.core`, and every file in every other module that
imports from core will need its imports updated accordingly.

## Scope

- **108 source files** with `package io.djb` or `import io.djb.*` declarations
- **9 Gradle build files** with module names, group ID, artifact coordinates, GitHub URLs
- **6 Markdown files** (README.md, CLAUDE.md, 4 module READMEs)
- **8 module directories** to rename
- **16+ package directories** (`io/djb` → `io/github/bpdbi`)
- **4 native-image config directories** under `META-INF/native-image/io.djb/`
- **2 docs** in `docs/` (may contain "Djb" references)

## Steps

### 1. Rename module directories

```bash
mv djb-bom             bpdbi-bom
mv djb-core            bpdbi-core
mv djb-pg-client       bpdbi-pg-client
mv djb-mysql-client    bpdbi-mysql-client
mv djb-kotlin          bpdbi-kotlin
mv djb-record-mapper   bpdbi-record-mapper
mv djb-javabean-mapper bpdbi-javabean-mapper
mv djb-pool            bpdbi-pool
```

### 2. Rename Java/Kotlin package directories

In every module, rename the `io/djb` directory tree to `io/github/bpdbi`, inserting the
subproject segment. The directory structure gains one level (`github`) and the leaf package
changes per the mapping table above.

**Core module** (special — root package gains `/core`):
```
bpdbi-core/src/main/java/io/djb/           → bpdbi-core/src/main/java/io/github/bpdbi/core/
bpdbi-core/src/main/java/io/djb/impl/      → bpdbi-core/src/main/java/io/github/bpdbi/core/impl/
bpdbi-core/src/test/java/io/djb/           → bpdbi-core/src/test/java/io/github/bpdbi/core/
bpdbi-core/src/test/java/io/djb/impl/      → bpdbi-core/src/test/java/io/github/bpdbi/core/impl/
bpdbi-core/src/testFixtures/java/io/djb/test/ → bpdbi-core/src/testFixtures/java/io/github/bpdbi/core/test/
```

**Other modules** (follow the mapping table):
```
bpdbi-pg-client/src/main/java/io/djb/pg/       → bpdbi-pg-client/src/main/java/io/github/bpdbi/pg/
bpdbi-mysql-client/src/main/java/io/djb/mysql/  → bpdbi-mysql-client/src/main/java/io/github/bpdbi/mysql/
bpdbi-kotlin/src/main/kotlin/io/djb/kotlin/     → bpdbi-kotlin/src/main/kotlin/io/github/bpdbi/kotlin/
bpdbi-record-mapper/src/main/java/io/djb/mapper/ → bpdbi-record-mapper/src/main/java/io/github/bpdbi/mapper/
bpdbi-javabean-mapper/src/main/java/io/djb/mapper/ → bpdbi-javabean-mapper/src/main/java/io/github/bpdbi/mapper/
bpdbi-pool/src/main/java/io/djb/pool/           → bpdbi-pool/src/main/java/io/github/bpdbi/pool/
examples/src/main/java/io/djb/examples/         → examples/src/main/java/io/github/bpdbi/examples/
```

(Same pattern for `src/test/` directories.)

### 3. Rename native-image resource directories

```
META-INF/native-image/io.djb/djb-record-mapper/
  → META-INF/native-image/io.github.bpdbi/bpdbi-record-mapper/

META-INF/native-image/io.djb/djb-javabean-mapper/
  → META-INF/native-image/io.github.bpdbi/bpdbi-javabean-mapper/
```

### 4. Update `settings.gradle.kts`

```kotlin
// Before
rootProject.name = "djb"
include("djb-bom", "djb-core", "djb-pg-client", ...)

// After
rootProject.name = "bpdbi"
include("bpdbi-bom", "bpdbi-core", "bpdbi-pg-client", ...)
```

### 5. Update root `build.gradle.kts`

- `group = "io.djb"` → `group = "io.github.bpdbi"`
- Module name list (`"djb-core"`, `"djb-pg-client"`, ...) → (`"bpdbi-core"`, `"bpdbi-pg-client"`, ...)
- `description = "djb — ..."` → `description = "bpdbi — ..."`
- All GitHub URLs: `github.com/cies/djb` → `github.com/cies/bpdbi`

### 6. Update module `build.gradle.kts` files

In each module's build file, update `project(":djb-core")` → `project(":bpdbi-core")`, etc.
Also update `testFixtures(project(":djb-core"))` → `testFixtures(project(":bpdbi-core"))`.

Files:
- `bpdbi-pg-client/build.gradle.kts`
- `bpdbi-mysql-client/build.gradle.kts`
- `bpdbi-kotlin/build.gradle.kts`
- `bpdbi-record-mapper/build.gradle.kts`
- `bpdbi-javabean-mapper/build.gradle.kts`
- `bpdbi-pool/build.gradle.kts`
- `bpdbi-bom/build.gradle.kts` (group, artifact names, GitHub URLs, description)
- `examples/build.gradle.kts`

### 7. Find-and-replace in all source files (~108 files)

Three passes, order matters:

1. **Core package** (most specific first to avoid partial matches):
   - `io.djb.impl` → `io.github.bpdbi.core.impl`
   - `io.djb.test` → `io.github.bpdbi.core.test`

2. **Other module packages** — these already have a subpackage so the replacement is
   straightforward:
   - `io.djb.pg` → `io.github.bpdbi.pg`
   - `io.djb.mysql` → `io.github.bpdbi.mysql`
   - `io.djb.kotlin` → `io.github.bpdbi.kotlin`
   - `io.djb.mapper` → `io.github.bpdbi.mapper`
   - `io.djb.pool` → `io.github.bpdbi.pool`
   - `io.djb.examples` → `io.github.bpdbi.examples`

3. **Remaining bare `io.djb`** references (core root package — after steps 1-2 only
   unqualified core references remain):
   - `io.djb` → `io.github.bpdbi.core`

   This covers:
   - `package io.djb` → `package io.github.bpdbi.core`
   - `import io.djb.Connection` → `import io.github.bpdbi.core.Connection`
   - `{@link io.djb.Connection}` → `{@link io.github.bpdbi.core.Connection}`
   - etc.

### 8. Update native-image config content

- `bpdbi-record-mapper/.../reflect-config.json`:
  `"name": "io.djb.mapper.RecordRowMapper"` → `"name": "io.github.bpdbi.mapper.RecordRowMapper"`
- `bpdbi-javabean-mapper/.../reflect-config.json`:
  `"name": "io.djb.mapper.JavaBeanRowMapper"` → `"name": "io.github.bpdbi.mapper.JavaBeanRowMapper"`

### 9. Update documentation

**README.md:**
- Title: `# Djb —` → `# Bpdbi —`
- All prose references to "Djb" → "Bpdbi"
- Maven coordinates: `io.djb:djb-*` → `io.github.bpdbi:bpdbi-*`
- Module list table
- GitHub URLs

**CLAUDE.md:**
- Title: `# djb —` → `# bpdbi —`
- Build commands: `./gradlew :djb-pg-client:test` → `./gradlew :bpdbi-pg-client:test`
- Module list
- Architecture notes

**Module READMEs** (`bpdbi-record-mapper/README.md`, `bpdbi-javabean-mapper/README.md`,
`bpdbi-kotlin/README.md`, `bpdbi-pool/README.md`):
- Titles, maven coordinates, module references

**docs/*.md** — scan for any "Djb" or "djb" references.

### 10. Update `.gitignore` and other config files

Check for any `djb`-specific entries (unlikely but verify).

### 11. Update examples

All files in `examples/src/main/java/io/djb/examples/` move to
`examples/src/main/java/io/github/bpdbi/examples/` and have their package declarations updated.

## Execution order

1. Rename directories first (steps 1-3) — do this in one batch
2. Global find-and-replace for packages in order (step 7) — most-specific first, bare `io.djb` last
3. Global find-and-replace `djb-` → `bpdbi-` for artifact/module names (steps 4-6, 8)
4. Global find-and-replace `"io.djb"` → `"io.github.bpdbi"` for Maven group ID in build files
5. Manual review of prose in Markdown files — replace "Djb" → "Bpdbi", "djb" → "bpdbi" (step 9)
6. Build and test: `./gradlew build -x test` then `./gradlew :bpdbi-core:test :bpdbi-record-mapper:test :bpdbi-javabean-mapper:test`
7. Run `spotlessApply` to fix any formatting issues from the rename

## Verification

```bash
# Should find zero matches after rename
grep -r "io\.djb[^a-z]" --include="*.java" --include="*.kt" --include="*.kts" --include="*.json" --include="*.properties"
grep -r '"djb-' --include="*.kts" --include="*.md"
grep -r "package io\.djb" --include="*.java" --include="*.kt"
# Build should pass
./gradlew build -x test
# Tests should pass
./gradlew test
```

## Risks

- **Git history** — directory renames may not track as renames in git if combined with content
  changes. Consider doing the `mv` in a separate commit before the content find-and-replace.
- **IDE caches** — IntelliJ may need "Invalidate Caches and Restart" after the rename.
- **Downstream consumers** — if anyone has already referenced `io.djb:djb-*` coordinates, they
  will break. Since the project is not yet published to Maven Central, this is low risk.
- **Core package shift** — the core root package moves from `io.djb` (no subpackage) to
  `io.github.bpdbi.core` (with `.core` segment). This means every module that imports core
  types needs `io.github.bpdbi.core.Connection` instead of just `io.djb.Connection`. The
  find-and-replace in step 7 handles this, but manual verification is important since a bare
  `io.djb` prefix match must become `io.github.bpdbi.core`, not `io.github.bpdbi`.
