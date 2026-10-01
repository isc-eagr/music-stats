# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

AGENTS.md (imported above) holds the shared project rules and domain notes so Codex and Claude Code read one source of truth. Everything below is Claude Code specific. If they conflict, this file wins. `.claude/settings.json` enforces the git and script rules (blocks push/pull/checkout/commit, prompts before running the .bat scripts).

## Commands
Run from the repo root. Java 25, Spring Boot 4.0.5, Maven wrapper (no linter or formatter is configured).

    ./mvnw.cmd -DskipTests package          # compile check, run as the last step of every change
    ./mvnw.cmd test                         # full suite
    ./mvnw.cmd -Dtest=CatalogFilterRegressionTest test
    ./mvnw.cmd -Dtest=CatalogFilterRegressionTest#artistLookupTagImageThemeAndItunesFiltersCoverAllModes test

Do not start the app. `music-stats.bat` kills any Java on port 8080 and runs the jar at the repo root; `deploy-prod.bat` runs `clean package` and copies `target/*.jar` over that root jar. Both are user-initiated only.

Read-only query benchmarks (no Spring, SQLite opened `mode=ro`) live in `tools/performance/`. `performance-optimization-report.md` summarizes the optimization work done so far. Do not reintroduce app-level timeframe caching.

## Architecture
**One SQLite file is the live database.** It sits outside the repo at `C:/Music Stats DB/music-stats.db` in WAL mode and is shared by the running app. DDL is `none`, so a new column needs three edits: a `migration_*.sql`, `db_consolidated.sql`, and `TestDatabaseSupport.createSchema()` (tests build their schema by hand, so a missing column there fails tests, not production).

**Catalog list pipeline.** A controller binds request params into a `*StatsQuery` record (`SongStatsQuery`, `ArtistStatsQuery`, `AlbumStatsQuery`). The repository builds one dynamic SQL string plus a params list using `util/SqlFilterHelper.appendXFilter(...)`, returns `*StatsRow`, and the service maps to `*CardDTO`. Every filter supports the includes / excludes / isnull / isnotnull modes, and `CatalogFilterRegressionTest` and `CatalogFilterEdgeCaseTest` exercise all of them, so extend those when adding a filter.

**Overrides live in SQL, not Java.** Song, album and artist attributes cascade with `COALESCE(a.override_genre_id, ar.genre_id)`. A filter must handle both branches: override present, and override NULL falling back to the parent (see `AlbumRepository` around the `override_genre_id` conditions). Getting only one branch right silently drops rows.

**Where the weight is.** Aggregation logic is concentrated in a few very large services: `ChartService` (~5k lines), `ArtistService`, `SongService`, `TimeframeService`, `AlbumService`. Timeframe, year and chart pages are the slow paths (many `COUNT(DISTINCT CASE ...)` over the full `Play` table), so measure before and after touching them.

**Runtime config is in the database.** `AppConfigService` creates and reads an `app_config` table itself, so settings are not in `application.properties`. `AutomatedPlayImportService` fires every minute via `@Scheduled` and is gated by that config (enabled flag, time window, interval). Any Spring context started against the real DB can therefore trigger imports, which is why there is deliberately no `@SpringBootTest` context-load test.

**Standalone maintenance scripts.** The classes directly under `src/main/java/library/*.java` (the `*Populator`, `PlayMatcherScript`, `PlaySyncScript`, `CaseDuplicateMerger`, `BillboardHot100ImportRunner`, and so on) each have their own `main()` and a hardcoded `DB_PATH` pointing at the live database. They write real data, so never run them without the user asking. They are separate from the Spring app, which starts only from `MusicLibraryApplication`.

**Tests do not use Spring or the real DB.** `TestDatabaseSupport` opens an in-memory SQLite (`mode=memory&cache=shared`), creates the schema, seeds a small catalog (artists such as Selena and Bad Bunny, with tags, themes, iTunes ids) and wires repositories by hand with a mocked `AppConfigService`. Follow that pattern for new repository or filter tests instead of adding `@SpringBootTest`.

**Frontend.** Server-rendered Thymeleaf under `src/main/resources/templates/<catalog>/`, with shared pieces in `fragments/`. Thymeleaf caching is off in `application.properties`. The Graphs page lives in `fragments/graphs-view.html` with `static/js/graphs.js`.
