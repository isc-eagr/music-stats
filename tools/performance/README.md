# Slow path benchmark

`SlowPathBenchmark.java` directly calls the timeframe service and the album
repository's list, count and gender-count operations. It does not start Spring,
the web server or background import jobs. SQLite is opened with `mode=ro` and
`PRAGMA query_only=ON`.

Run from the repository root in PowerShell:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-25.0.3'
.\mvnw.cmd -q -DskipTests compile dependency:build-classpath '-Dmdep.outputFile=target/performance-classpath.txt' '-DincludeScope=runtime'
New-Item -ItemType Directory -Force target/benchmark-native | Out-Null
$benchmarkCp = 'target/classes;' + (Get-Content target/performance-classpath.txt -Raw).Trim()
& "$env:JAVA_HOME\bin\java.exe" --enable-native-access=ALL-UNNAMED '-Dorg.sqlite.tmpdir=C:/Code/music-stats/target/benchmark-native' -cp $benchmarkCp tools/performance/SlowPathBenchmark.java 'C:/Music Stats DB/music-stats.db' target/slow-paths 2 extended
```

Arguments: database file, output prefix, repetitions (default 2), optional path
substring or `extended`. Without the last argument, the ten slowest paths from
the audit are measured. Extended mode also measures every timeframe landing page,
every timeframe's winning-gender filter and the three full-listen album sorts.
Requests are sequential. Outputs include timings in TSV and complete result
snapshots for comparing counts, order and card fields between implementations.
Snapshot files can contain music-library data and belong under `target/`.

For a valid before/after comparison, keep a copy of the original compiled classes
before changing the implementation, then put that directory before `target/classes`
on the baseline classpath. Run old and new implementations sequentially with the
same arguments and database. A live import or metadata edit may change results
between runs; investigate differences before treating them as regressions.

These measurements cover database and service/repository work. They exclude
controller enrichment, HTML rendering, network transfer and browser rendering.
Use HTTP measurements after the app is restarted by its operator to establish
actual page load time. Do not compare these timings directly with earlier HTTP
measurements as if they measured the same thing.

## Round two: secondary catalogs

`CatalogPathBenchmark.java` measures the ten selected Languages, Countries,
Genres and Ethnicities paths twice, plus ten related search/sort cases once.
Each sample includes the full catalog service call and the controller's separate
total-count operation. Individual query-stage timings are also recorded.

After building the runtime classpath with the command above:

```powershell
New-Item -ItemType Directory -Force target/performance-tools | Out-Null
& "$env:JAVA_HOME\bin\javac.exe" -cp $benchmarkCp -d target/performance-tools tools/performance/SlowPathBenchmark.java tools/performance/CatalogPathBenchmark.java
& "$env:JAVA_HOME\bin\java.exe" --enable-native-access=ALL-UNNAMED '-Dorg.sqlite.tmpdir=C:/Code/music-stats/target/benchmark-native' -cp "target/performance-tools;$benchmarkCp" CatalogPathBenchmark 'C:/Music Stats DB/music-stats.db' target/catalog-paths 2
```

An optional fourth argument limits the run to paths containing that string.
The round-two baseline classes were copied to
`target/performance-round2-before/`; put that directory first on the classpath to
run the original implementation. This directory and raw measurements are local
build artifacts: cleaning `target/` removes them. The Markdown report retains the
timing tables independently of those artifacts.

## Round three: year catalogs and complete detail models

`DetailPathBenchmark.java` invokes the actual controllers directly, including all
eagerly loaded tabs, rankings, heatmaps, play history, and group/featured variants.
It records each JDBC query and the complete model. It starts no application,
scheduled jobs, or server. Template rendering and browser/network time are excluded.

Compile it alongside `SlowPathBenchmark.java`; run with the same classpath as the
catalog runner. Arguments are database file, output prefix, repetitions, and an
optional path substring (`details` selects all detail cases; `extended` selects
additional coverage). The existing iTunes XML cache is warmed before timing.

For a consistent comparison while imports are active, use a SQLite backup snapshot.
Round three uses `target/performance-round3-snapshot.db`; original compiled classes
are in `target/performance-round3-before/`. Run the baseline with that directory
before `target/classes` in the classpath and without the two new indexes **on the
snapshot only**. Apply `db_detail_performance_indexes.sql` to the snapshot and run
the current classes. Compare the `*-results.txt` files and timing TSVs. Both indexes
were also applied to the local working database. Other installations need the SQL
script applied separately. The temporary 8.9 GB snapshot was removed after the
30 complete model comparisons passed; recreate it with SQLite's backup API for
another run. Raw timings, query stages, model outputs, and baseline classes remain
under `target/` until the next clean build.
