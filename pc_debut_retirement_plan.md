# Personal Cuntdown `pc_debut` Retirement Plan

`pc_debut` is still acting as the canonical identity layer for Personal Cuntdown. The overview page can already be derived from `pc_countdown_entry`, but recaps, matching, Hall of Fame state, and the crawler pipeline still depend on `pc_debut` and on `pc_countdown_entry.debut_id`.

## Current blockers

1. Matching and unmatching still write to `pc_debut`.
Files:
`src/main/java/library/service/PcService.java`

2. Hall of Fame / retired state still lives only on `pc_debut.retired`.
Files:
`src/main/java/library/service/PcService.java`
`src/main/resources/templates/misc/pc.html`
`src/main/resources/templates/misc/pc-recaps.html`

3. Recap queries still resolve linked songs and aggregate stats through `ce.debut_id -> pc_debut.id`.
Files:
`src/main/java/library/service/PcService.java`

4. Merge flow still depends on merging two `pc_debut` identities and reassigning `pc_countdown_entry.debut_id`.
Files:
`src/main/java/library/service/PcService.java`
`src/main/java/library/controller/PcController.java`

5. Import generation still rebuilds `pc_debut` and then links `pc_countdown_entry.debut_id` back to it.
Files:
`src/main/java/library/PersonalCuntdownCrawler.java`
`pc_crawl_output.sql`
`migration_personal_cuntdown.sql`

6. JPA/entity wiring still exposes `pc_debut` as a first-class table.
Files:
`src/main/java/library/entity/PcDebut.java`
`src/main/java/library/repository/PcDebutRepository.java`

## Recommended target model

Keep `pc_countdown_entry` as the only raw/history table and replace `pc_debut` with a thinner canonical group table whose purpose is explicit.

Recommended replacement table: `pc_song_group`

Columns:

1. `id`
2. `canonical_artist_name`
3. `canonical_song_title`
4. `song_id`
5. `retired`

Then rename `pc_countdown_entry.debut_id` to `group_id` once the app logic is switched.

This keeps one canonical identity row per normalized PC song while removing the misleading “debut” meaning from the schema.

## Migration phases

### Phase 1: Stop treating the overview as `pc_debut`-backed

Status: done for the main overview page.

Completed work:

1. Main PC overview is derived from `pc_countdown_entry`.
2. Matching raw groups from the overview already updates both raw rows and `pc_debut`.

### Phase 2: Introduce the replacement canonical table

Add a migration that creates `pc_song_group` with:

1. `id INTEGER PRIMARY KEY AUTOINCREMENT`
2. `canonical_artist_name TEXT NOT NULL`
3. `canonical_song_title TEXT NOT NULL`
4. `song_id INTEGER NULL`
5. `retired INTEGER NOT NULL DEFAULT 0`

Also add `group_id` to `pc_countdown_entry` and backfill it from the existing `debut_id` values.

At the end of this phase both references exist in parallel:

1. `pc_countdown_entry.debut_id`
2. `pc_countdown_entry.group_id`

### Phase 3: Move write paths to the new canonical table

Update `PcService` so these methods stop writing `pc_debut`:

1. `matchRawGroup`
2. `matchSong`
3. `unmatchSong`
4. `mergeDebuts` renamed to something like `mergeGroups`
5. `autoLinkExactMatches`

Behavior change:

1. Canonical names and `song_id` are written to `pc_song_group`.
2. `pc_countdown_entry.group_id` is the only identity pointer used for stats and recap joins.
3. Merge rewires `group_id`, not `debut_id`.

### Phase 4: Move read paths off `pc_debut`

Update all PC reads to use `pc_song_group` plus `pc_countdown_entry.group_id`:

1. Overview summary/grouping queries
2. Song detail PC chip stats
3. Recap page query
4. Hall of Fame filtering

Important rule:

`days_on_countdown` should become fully derived, not stored.

That means any fallback like `SELECT SUM(days_on_countdown) FROM pc_debut` should be removed and replaced with derived counts from raw rows.

### Phase 5: Update crawler/import artifacts

Change the crawler SQL output so step 2 builds `pc_song_group` instead of `pc_debut`, then updates `pc_countdown_entry.group_id`.

Files to change:

1. `src/main/java/library/PersonalCuntdownCrawler.java`
2. `pc_crawl_output.sql`
3. `migration_personal_cuntdown.sql`

### Phase 6: Delete `pc_debut`

Only after phases 2 through 5 are done:

1. Drop foreign key/index usage of `debut_id`
2. Drop `pc_debut`
3. Remove `PcDebut` entity
4. Remove `PcDebutRepository`
5. Rename remaining code symbols from `debut` to `group`

## Safe cutover checklist

Before deleting `pc_debut`, verify all of these are true:

1. No SQL in `src/main/java/**` references `pc_debut`
2. No SQL in repo scripts references `pc_debut`
3. `pc_countdown_entry` no longer uses `debut_id`
4. Matching/unmatching/merge flows work through the replacement group table
5. Recaps still show `song_id`, retired state, peak stats, and movement correctly
6. Song detail PC chips still aggregate correctly
7. Crawler output rebuilds the replacement table from scratch

## Suggested implementation order

1. Add `pc_song_group` and `group_id`
2. Backfill from existing `pc_debut`
3. Switch service write paths
4. Switch recap/stat read paths
5. Switch crawler output
6. Remove `pc_debut`

This is the safest path because it keeps current data intact while letting the app migrate incrementally without breaking recaps or manual linking.