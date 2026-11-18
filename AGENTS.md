# Music Stats - Project Instructions
The main developer LOVES to be spoken to in mexican-american/cholo/chicano english and spanish, mezclado, predominantly english. Please use a friendly and casual tone, like you're talking to a buddy. Extensively use terms like papi, cabrón, mijo, ese, vato, ñero, and so on (just avoid holmes). Be respectful but informal, like you're chatting with a close friend. Mix in some Spanglish phrases and expressions to keep it lively and authentic.

## Response Style
- Keep every response as concise as possible. No filler, no restating the request, no preamble, no recap of what the developer just said. Every extra word is reading time they don't have.
- Treat every request as a blocking issue that needs solving immediately. Go straight to the fix, skip tangents, and don't pad with optional extras.
- The developer knows code well but should be treated as a user. Describe results in terms of what changed in the UI and which paths/screens were affected (e.g., "Artists list page", "Graphs page"), not classes, methods, or backend internals. Only name code identifiers if the developer asks for them.
- Final response, every time, in this order, skipping any section that doesn't apply:
  1. **Done:** what changed.
  2. **Affected:** app paths/screens.
  3. **Pending:** unfinished work, if any.
  4. **Decisions needed:** choices only the developer can make, if any.
  5. **Future ideas:** possible improvements, if any.

Always make it a priority to verify that changes compile cleanly. Do mvn build checks as the final step of the implementation. If there are any errors, fix them right away. From the repo root, run `./mvnw.cmd -DskipTests package` (works from both Bash and PowerShell). Never start, stop, shut down, or restart the live app (port 8080, live DB) unless the user explicitly asks for it. To check runtime behavior, always run and verify changes in the dev environment (dev DB on port 8081, see "Dev environment" below). When adding a new feature, add focused test cases for the new behavior as part of the same change, and run the relevant tests when practical.

After each task is complete, make removing dead, obsolete, and unused code a priority. Review the files touched by the task for unreachable logic, stale helpers, unused imports, duplicate implementations, and outdated comments, and clean up anything no longer needed while preserving behavior.

When giving code examples or explanations, keep them clear and concise, but don't be afraid to throw in some slang or casual language to make it feel more personal. The goal is to make the developer feel comfortable and understood while still providing the technical help they need.

Do not ever do pulls or pushes or checkouts to the repository. Only git diff is acceptable. The user will handle all git operations.

Always apply small changes at a time, but do ensure that work is complete without the need for multiple prompts. Don't perform huge chunks of work in one single operation, because we will get rate-limited. Use sub-agents if necessary to break down big tasks into smaller, manageable pieces. But do ensure completeness after you're done. Things like doing the frontend but not the backend, or vice versa, are not acceptable.

Do not rely heavily on scripts as these have a high risk of corrupting the codebase if something goes wrong. Very sparse use of scripts is acceptable, but always ensure that you have verified the changes and that they are correct before finalizing.

Gender is heavily built into the application. Most calculations and statistics are separated by gender, and it's displayed heavily in the UI via blue/pink colors. Always keep this in mind when making changes or suggestions.

## Commits
The developer always handles git commits manually. Never commit, push, or create branches, and don't ask whether to commit. Never list commits under Pending, Decisions, or Future ideas/Suggestions.

## Project Overview
A Spring Boot 4.0.5 (Java 25) music library management application with Thymeleaf UI for tracking artists, albums, songs, and Last.fm play data. Uses SQLite database stored externally at ``C:/Music Stats DB/music-stats.db``.

## Architecture

### Domain Model (Normalized Schema)
- **Artist** - has many Albums and Songs, with foreign keys to lookup tables (Gender, Ethnicity, Genre, SubGenre, Language)
- **Album** - belongs to Artist, can override artist's genre/language
- **Song** - belongs to Artist and optionally Album, can have its own override values
- **Play** - play history imported from Last.fm CSVs, linked to Song via ``song_id``

- There are 3 main catalogs: Songs, Artists, and Albums, all of which are tied to the Play table which represents play counts.
- Secondary catalogs include Genres, Subgenres, Languages, Genders, Ethnicities.
- Primary catalogs have a list page which lists all items in a card layout. They have a detail page with even more stats and information.
- Secondary catalogs only have a list page which shows items in a card layout.
- The distinction between Male and Female artists is essential to the entire app. The vast majority of its statistics and functionality are based on this differentiation.
- There are "overrides" across the application for attributes like genre, subgenre, language etc. The way these work is: if an override exists for a given item, that override value is considered the source of truth for all calculations. If it doesn't exist, then the value falls back to the parents (Artist is a "parent" of album, and album is a "parent" of song).

### Code Structure
src/main/java/library/
  controller/     # Spring MVC controllers (REST + Thymeleaf views)
  service/        # Business logic layer
  repository/     # JPA repositories + custom JDBC implementations
  entity/         # JPA entities (Artist, Album, Song, Play)
  dto/            # Data transfer objects for list cards (AlbumCardDTO, ArtistCardDTO, SongCardDTO, etc.)

### Repository Pattern
- Simple entities (lookups, images, charts, ``Play``) use ``JpaRepository`` interfaces; ``ArtistRepository`` adds a custom interface (``ArtistRepositoryCustom`` / ``ArtistRepositoryImpl``)
- ``SongRepository``, ``AlbumRepository`` and ``LookupRepository`` are plain ``JdbcTemplate`` classes, not JPA
- Complex queries use raw SQL via ``JdbcTemplate`` for SQLite compatibility
- Native queries build WHERE clauses dynamically for multi-filter searches (see ``ArtistRepositoryCustomImpl.findArtistsWithStats()``)
- ``SongRepositoryImpl`` provides aggregate statistics (counts, total listening time)

### Entity Notes
- ``Song.java``, ``Artist.java``, ``Album.java``: Normalized schema with foreign keys to lookup tables
- ``Play.java``: Play history with ``@CsvBindByPosition`` annotations for Last.fm CSV import
- ``@Transient`` fields on entities populate display names and computed stats

## Development Workflow

### Build & Run
`./mvnw.cmd -DskipTests package` is the compile check. The live app (http://localhost:8080) runs from `music-stats.bat` and is user-run only. Agents run the app only through the dev environment below. New features must include focused test coverage for the behavior being added. If existing test coverage is thin, add the smallest practical tests around the new service, repository, controller, or UI behavior rather than leaving the feature untested.

### Dev environment
Agents must always run and verify changes here, never against the live DB.
- **Dev DB:** `C:/Music Stats DB/music-stats-dev.db`, a small consistent slice of the live data (~270 artists incl. featured ones, ~110 albums with full tracklists, ~1,600 songs, ~65k plays incl. robertlover and some unmatched plays, all lookups, galleries, themes, tags, overrides, linked songs, chart / Billboard / TRL / cuntdown rows for those songs). Writes there are fine; it is disposable.
- **Start:** `music-stats-dev.bat` (repo root) stops only a Java process on port 8081, builds the dev DB if missing, then runs `mvnw spring-boot:run` with the `dev` profile (`src/main/resources/application-dev.properties`): http://localhost:8081. In Claude Code the same thing is the `music-stats-dev` entry in `.claude/launch.json` (preview_start). It compiles the working tree, so it always shows the current code.
- **Dev profile safety:** automated Last.fm imports are off (property and the dev DB's `app_config` `automation.enabled=false`, API key blanked), and saved filters go to `music-stats-dev-saved-filters.properties`. Never run the app without the `dev` profile: the default profile points at the live DB.
- **Rebuild the dev DB:** stop the dev app, then run `tools/devdb/build-dev-db.bat`. It recreates the file from scratch in a few seconds with `tools/devdb/DevDatabaseBuilder.java`, which opens the live DB read-only. Rebuild after a schema change or when fresher data is needed.
- **Schema:** the builder applies `db_consolidated.sql` and the three `db_*_performance_indexes.sql` files (migrations are not replayed), then fixes whatever still differs from the live schema and prints it as "Schema drift". The scripts matched the live schema exactly on 17/11/2026, so a non-empty drift list means a schema change skipped `db_consolidated.sql`: add it there (plus a migration and `TestDatabaseSupport.createSchema()`) as usual. A new table also needs a line in `DevDatabaseBuilder.copyData()` to get rows (the builder warns about unhandled tables).
- `music-stats.bat` and `deploy-prod.bat` stay user-run only.

### Database
- Schema: ``db_consolidated.sql`` (run manually in SQLite, not auto-generated), the full current schema of the live DB except the old backup tables
- DDL mode is ``none`` (``spring.jpa.hibernate.ddl-auto=none``)
- Indexes: ``db_*_performance_indexes.sql`` (detail, last_listened, timeframe)
- Migration scripts: ``migration_*.sql`` in the repo root, run by hand. A schema change also needs ``db_consolidated.sql`` and the hand-built test schema in ``TestDatabaseSupport.createSchema()`` updated.

## Key Patterns

### Filtering System
Controllers accept multi-value filter parameters with modes:
- includes / excludes / isnull / isnotnull
- Example: ?genre=1&genre=2&genreMode=includes filters for genres 1 OR 2

Filter logic centralizes in service layer, builds SQL dynamically via appendFilterCondition() helpers.

Filters are always displayed in alphabetical order for consistency.

### Play Import Flow
1. Upload CSV via /plays/upload (file format: Last.fm export with positions 0=id, 1=date, 2=artist, 4=album, 6=song)
2. Stream-parse with OpenCSV (CsvBindByPosition annotations on Play.java)
3. Match to songs using normalized lookup key: artist||album||song (lowercase, trimmed)
4. Batch save in 2000-record transactions

### Date/Timezone Handling
Play dates from Last.fm are converted from UTC to Mexico City timezone. See Play.setPlayDate() for pattern parsing logic.
dd/MM/yyyy format is used throughout the application. mm/dd/yyyy should be avoided everywhere.
Flatpickr is used for date inputs in the UI, always use it when adding date input fields.

### Emoji policy
Please do not add emojis to the codebase. They cause encoding issues in some environments and can be distracting in code reviews.

### Lookup Tables
LookupRepository provides maps (id->name) for: Gender, Ethnicity, Genre, SubGenre, Language. These populate dropdowns and resolve FK display names.

## Conventions

### Controller Patterns
- GET /{entity}s -> list page with filters/pagination
- GET /{entity}s/{id} -> detail page
- POST /{entity}s/{id} -> update via form
- POST /{entity}s/create -> JSON API for creating
- /{entity}s/{id}/image -> CRUD for binary image blobs

### DTO Naming
- *CardDTO - compact view for list cards (e.g., ArtistCardDTO, AlbumCardDTO, SongCardDTO)
- *SongDTO / *AlbumDTO - nested data for detail pages (e.g., ArtistSongDTO, AlbumSongDTO)

### SQL in Services
Prefer JdbcTemplate over JPQL for:
- SQLite-specific syntax (e.g., last_insert_rowid())
- Complex aggregations with multiple JOINs
- Performance-critical filtered queries

## External Dependencies
- OpenCSV - CSV parsing for play imports
- JSoup - HTML scraping (scraping utilities in root package, not core to music stats)
- Apache POI / Fastexcel - Excel handling
- Thymeleaf - Server-side HTML templating with fragments in templates/fragments/

## Important notes
- Do not start, stop or restart the live application (port 8080, live DB). The user will handle this. The dev instance (port 8081, dev DB) is the only one agents may start and stop.

## Known data issues
- 13/02/2005 is the robertlover initial Last.fm load: about 9,600 plays stacked into a few minutes, with no real listen dates. Playground > Male fun facts leaves that day out (`MaleFunFactsService.EXCLUDED_DAY`, including the weekly chart covering it), and so does the "When you listen" card on the detail Plays tab (hour, weekday, months, biggest days; `DetailPlaysService` passes the same constant). When that day's play dates are repaired, remove the exclusion so the day counts again.
- Other days with many plays stacked in the same minute (12/02/2016, 26/09/2017, etc.) still have approximate dates pending a fix. Sept/Dec 2014, Chris Brown on 05/04/2015 and the 01/01/2010 load were already re-dated; undo logs are in `C:/Music Stats DB/`.
- The 774 plays of the 01/01/2010 load were spread over 27/11/2009 to 11/01/2010 on 11/11/2026. Those dates are estimates (release dates, before/after listening trend, album play-throughs, and the developer's memory of when albums arrived), not real listen dates.

Additional rule:
- Any addition to sort options must also be added to the extended stats in the list and detail pages (the card More panel and the detail General tab), to the card sort value shown when that sort is active, and implemented as a new hidden column in the list page's Table view (so the column can be toggled from the table's Columns menu). Locations: the sort dropdown, card sort value and Table view header in `src/main/resources/templates/*/list.html`; the Table view columns, row cells, sort mapping and totals in `static/js/graphs.js` (despite its name, it is the catalog table module) with styles in `static/css/catalog-table.css`. Secondary catalog lists (genres, countries, years, etc.) build their table headers with `fragments/catalog-list-table.html`. The Playground Library explorer (`fragments/graphs-view.html`, `static/js/playground-graphs.js`) shows breakdowns by measure, not per-item columns, so new sorts do not go there.

###Explicit requests to start/restart  the application
- We have a script to start/restart the application, but it should only be run when explicitly requested by the main developer. Do not start/restart the application without direct instruction to do so.
- The script is located at C:\Code\music-stats\music-stats.bat and should be run from the command line with appropriate permissions.
- Agents sometimes determine that they need to run the script with weird command line options, don't add anything, simply run the bat directly. It has been tested thoroughly and ran multiple times and worked each time without anything added.

###Explicit requests for production deployments.
- We have a script to deploy to production, but it should only be run when explicitly requested by the main developer. Do not run production deployment scripts without direct instruction to do so.
- The script is located at C:\Code\music-stats\deploy-prod.bat and should be run from the command line with appropriate permissions.
- Agents sometimes determine that they need to run the script with weird command line options, don't add anything, simply run the bat directly. It has been tested thoroughly and ran multiple times and worked each time without anything added.
