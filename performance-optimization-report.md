# Performance work

## Earlier rounds

- Rewrote the timeframe, secondary catalog, listen/release year, album full-listen and detail page queries.
- Index scripts, applied by hand (other installations need them too): `db_timeframe_performance_indexes.sql`,
  `db_last_listened_performance_indexes.sql`, `db_detail_performance_indexes.sql`.
- App-level timeframe caching was tried and removed. Don't reintroduce it.

## Round four (29/09/2026)

- **SQLite connection** (`application.properties`): 64 MB page cache, in-memory temp store, memory-mapped reads.
  Stop the app before running VACUUM from another tool: a mapped database file can't be shrunk.
- **Windows efficiency mode**: `ProcessQosConfigurer` opts the app's process out of EcoQoS at startup, so requests
  stay off throttled efficiency cores. Disable with `--musicstats.performance.high-qos=false`. The jar manifest
  enables native access for it and for sqlite-jdbc.
- **Banners**: `AutomationBannerInterceptor` computes the automation and missing-weekly-chart banners only when an
  HTML page renders (it was a `@ControllerAdvice` that ran before every request, images and JSON included). The
  missing-weeks query uses a loose index scan.
- **Cover thumbnails**: `ThumbnailCache` keeps resized thumbnails in an in-memory LRU keyed by image content;
  `ImageResponses` adds ETags, `Cache-Control: no-cache` (unchanged covers return 304) and real content types.
- **Top Played Reigns**: the top 3 is maintained incrementally while plays stream from the index.
- **Album full listens**: one streaming pass over plays, shared by the list, count and gender-count queries of a request.
- **Timeframes**: song counts as sums, a materialized top-items base, and top items, winning attributes and male days
  loaded concurrently on the connection pool.
- **Secondary catalogs**: top items and gender totals count plays per song before joining metadata.

## Left for later

Timeframe pages sorted or filtered by male days or winning attributes are the slowest remaining pages.

## Tools

`tools/performance/` holds read-only Java harnesses that call the controllers or services directly (no Spring
context, no server, database opened with `mode=ro`). `PageProfiler` records every SQL statement a page runs.
Build a runtime classpath with `mvnw dependency:build-classpath` and see each class's comment for arguments.
