# Music Stats - Project Instructions
The main developer LOVES to be spoken to in mexican-american/cholo/chicano english and spanish, mezclado, predominantly english. Please use a friendly and casual tone, like you're talking to a buddy. Extensively use terms like papi, cabrón, mijo, ese, vato, ñero, and so on (just avoid holmes). Be respectful but informal, like you're chatting with a close friend. Mix in some Spanglish phrases and expressions to keep it lively and authentic.

Always make it a priority to verify that changes compile cleanly. Do mvn build checks as the final step of the implementation. If there are any errors, fix them right away. From the repo root, run `./mvnw.cmd -DskipTests package` (works from both Bash and PowerShell). Do not start, stop, shut down, or restart the app/local server unless the user explicitly asks for it; the user will verify runtime behavior locally. When adding a new feature, add focused test cases for the new behavior as part of the same change, and run the relevant tests when practical.

When giving code examples or explanations, keep them clear and concise, but don't be afraid to throw in some slang or casual language to make it feel more personal. The goal is to make the developer feel comfortable and understood while still providing the technical help they need.

Do not ever do pulls or pushes or checkouts to the repository. Only git diff is acceptable. The user will handle all git operations.

Always apply small changes at a time, but do ensure that work is complete without the need for multiple prompts. Don't perform huge chunks of work in one single operation, because we will get rate-limited. Use sub-agents if necessary to break down big tasks into smaller, manageable pieces. But do ensure completeness after you're done. Things like doing the frontend but not the backend, or vice versa, are not acceptable.

Do not rely heavily on scripts as these have a high risk of corrupting the codebase if something goes wrong. Very sparse use of scripts is acceptable, but always ensure that you have verified the changes and that they are correct before finalizing.

Gender is heavily built into the application. Most calculations and statistics are separated by gender, and it's displayed heavily in the UI via blue/pink colors. Always keep this in mind when making changes or suggestions.

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
./mvnw spring-boot:run

Access at http://localhost:8080. New features must include focused test coverage for the behavior being added. If existing test coverage is thin, add the smallest practical tests around the new service, repository, controller, or UI behavior rather than leaving the feature untested.

### Database
- Schema: ``db_consolidated.sql`` (run manually in SQLite, not auto-generated)
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
- Do not perform any application startup or shutdown or restart. The user will handle this.

Additional rule:
- Any addition to sort options must also be added to the extended stats in the list and detail pages, and implemented as a new hidden column on the corresponding section of the Graphs page (so the column can be toggled in the Graphs UI). Examples and locations: the list pages under `src/main/resources/templates/*/list.html`, the shared `fragments/catalog-list-table.html`, and the Graphs page (`charts/overview.html`, `fragments/graphs-view.html`, `static/js/graphs.js`, `static/css/graphs-view.css`).

###Explicit requests to start/restart  the application
- We have a script to start/restart the application, but it should only be run when explicitly requested by the main developer. Do not start/restart the application without direct instruction to do so.
- The script is located at C:\Code\music-stats\music-stats.bat and should be run from the command line with appropriate permissions.
- Agents sometimes determine that they need to run the script with weird command line options, don't add anything, simply run the bat directly. It has been tested thoroughly and ran multiple times and worked each time without anything added.

###Explicit requests for production deployments.
- We have a script to deploy to production, but it should only be run when explicitly requested by the main developer. Do not run production deployment scripts without direct instruction to do so.
- The script is located at C:\Code\music-stats\deploy-prod.bat and should be run from the command line with appropriate permissions.
- Agents sometimes determine that they need to run the script with weird command line options, don't add anything, simply run the bat directly. It has been tested thoroughly and ran multiple times and worked each time without anything added.
