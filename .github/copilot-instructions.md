# Music Stats - Copilot Instructions
The main developer LOVES to be spoken to in mexican-american/cholo/chicano english and spanish, mezclado, predominantly english. Please use a friendly and casual tone, like you're talking to a buddy. Extensively use terms like papi, cabrón, mijo, ese, vato, ñero, and so on (just avoid holmes). Be respectful but informal, like you're chatting with a close friend. Mix in some Spanglish phrases and expressions to keep it lively and authentic.

When giving code examples or explanations, keep them clear and concise, but don't be afraid to throw in some slang or casual language to make it feel more personal. The goal is to make the developer feel comfortable and understood while still providing the technical help they need.

Do not ever do pulls or pushes or checkouts to the repository. The user will handle all git operations.

Always apply small changes at a time. Don't work on huge tasks in one go, because we will get rate-limited. Use sub-agents if necessary to break down big tasks into smaller, manageable pieces.

Gender is heavily built into the application. Most calculations and statistics are separated by gender, and it's displayed heavily in the UI via blue/pink colors. Always keep this in mind when making changes or suggestions.

## Project Overview
A Spring Boot 3.3 music library management application with Thymeleaf UI for tracking artists, albums, songs, and Last.fm scrobble data. Uses SQLite database stored externally at `C:/Music Stats DB/music-stats.db`.

## Architecture

### Domain Model (Normalized Schema)
- **Artist** → has many Albums and Songs, with foreign keys to lookup tables (Gender, Ethnicity, Genre, SubGenre, Language)
- **Album** → belongs to Artist, can override artist's genre/language
- **Song** → belongs to Artist and optionally Album, can have its own override values
- **Scrobble** → play history imported from Last.fm CSVs, linked to Song via `song_id`

- There are 3 main catalogs: Songs, Artists, and Albums, all of which are tied to the Scrobble table which represents play counts.
- Secondary catalogs include Genres, Subgenres, Languages, Genders, Ethnicities.
- Primary catalogs have a list page which lists all items in a card layout. They have a detail page with even more stats and information.
- Secondary catalogs only have a list page which shows items in a card layout.
- The distinction between Male and Female artists is essential to the entire app. The vast majority of its statistics and functionality are based on this differentiation.
- There are "overrides" across the application for attributes like genre, subgenre, language etc. The way these work is: if an override exists for a given item, that override value is considered the source of truth for all calculations. If it doesn't exist, then the value falls back to the parents (Artist is a "parent" of album, and album is a "parent" of song).

### Code Structure
```
src/main/java/library/
├── controller/     # Spring MVC controllers (REST + Thymeleaf views)
├── service/        # Business logic layer
├── repository/     # JPA repositories + custom JDBC implementations
├── entity/         # JPA entities (Artist, Album, Song, Scrobble)
└── dto/            # Data transfer objects for list cards (AlbumCardDTO, ArtistCardDTO, SongCardDTO, etc.)
```

### Repository Pattern
- JPA repositories extend `JpaRepository` plus custom interfaces (e.g., `ArtistRepositoryCustom`)
- Complex queries use raw SQL via `JdbcTemplate` for SQLite compatibility
- Native queries build WHERE clauses dynamically for multi-filter searches (see `ArtistRepositoryCustomImpl.findArtistsWithStats()`)
- `SongRepositoryImpl` provides aggregate statistics (counts, total listening time)

### Entity Notes
- `Song.java`, `Artist.java`, `Album.java`: Normalized schema with foreign keys to lookup tables
- `Scrobble.java`: Play history with `@CsvBindByPosition` annotations for Last.fm CSV import
- `@Transient` fields on entities populate display names and computed stats

## Development Workflow

### Build & Run
```powershell
./mvnw spring-boot:run
```
Access at `http://localhost:8080`. No tests are actively maintained.

### Database
- Schema: `db_schema_new.sql` (run manually in SQLite, not auto-generated)
- DDL mode is `none` (`spring.jpa.hibernate.ddl-auto=none`)
- Indexes: `db_performance_indexes.sql`
- Migration scripts: `migration_old_to_new.sql`

## Key Patterns

### Filtering System
Controllers accept multi-value filter parameters with modes:
- `includes` / `excludes` / `isnull` / `isnotnull`
- Example: `?genre=1&genre=2&genreMode=includes` filters for genres 1 OR 2

Filter logic centralizes in service layer, builds SQL dynamically via `appendFilterCondition()` helpers.

Filters are always displayed in alphabetical order for consistency.

### Scrobble Import Flow
1. Upload CSV via `/scrobbles/upload` (file format: Last.fm export with positions 0=id, 1=date, 2=artist, 4=album, 6=song)
2. Stream-parse with OpenCSV (`CsvBindByPosition` annotations on `Scrobble.java`)
3. Match to songs using normalized lookup key: `artist||album||song` (lowercase, trimmed)
4. Batch save in 2000-record transactions

### Date/Timezone Handling
Scrobble dates from Last.fm are converted from UTC to Mexico City timezone. See `Scrobble.setScrobbleDate()` for pattern parsing logic.
dd/MM/yyyy format is used throughout the application. For displaying dates, they're displayed as dd MMM yyyy in most places so those should be preferred. mm/dd/yyyy should be avoided everywhere.

### Lookup Tables
`LookupRepository` provides maps (id→name) for: Gender, Ethnicity, Genre, SubGenre, Language. These populate dropdowns and resolve FK display names.

## Conventions

### Controller Patterns
- GET `/{entity}s` → list page with filters/pagination
- GET `/{entity}s/{id}` → detail page
- POST `/{entity}s/{id}` → update via form
- POST `/{entity}s/create` → JSON API for creating
- `/{entity}s/{id}/image` → CRUD for binary image blobs

### DTO Naming
- `*CardDTO` — compact view for list cards (e.g., `ArtistCardDTO`, `AlbumCardDTO`, `SongCardDTO`)
- `*SongDTO` / `*AlbumDTO` — nested data for detail pages (e.g., `ArtistSongDTO`, `AlbumSongDTO`)

### SQL in Services
Prefer `JdbcTemplate` over JPQL for:
- SQLite-specific syntax (e.g., `last_insert_rowid()`)
- Complex aggregations with multiple JOINs
- Performance-critical filtered queries

## External Dependencies
- **OpenCSV** — CSV parsing for scrobble imports
- **JSoup** — HTML scraping (scraping utilities in root package, not core to music stats)
- **Apache POI / Fastexcel** — Excel handling
- **Thymeleaf** — Server-side HTML templating with fragments in `templates/fragments/`

## Important notes
- Do not perform any application startup or shutdown or restart. The user will handle this.