# Timeframe Performance Retry Notes

Context: the machine was under heavy load during the 2026-06-15 investigation, so
the service-level timings were noisy. Re-run these when the machine is quieter.

## Baseline First

- Measure each page twice after startup and use the second load:
  - `http://localhost:8080/months`
  - `http://localhost:8080/seasons`
  - `http://localhost:8080/years`
  - `http://localhost:8080/decades`
- Also capture a service-level timing if useful, but trust browser/HTTP timings more
  because Thymeleaf/controller/chart work also contributes to page load.
- Do not reintroduce app-level timeframe caching unless user constraints change.

## Approaches Worth Retrying

1. Pre-aggregate the main summary by period + song.
   - Shape tested:
     - `song_period_counts`: group plays by `period_key, s.id`.
     - `period_summary`: derive play/time counts and distinct artist/album/song/gender stats from that smaller rowset.
   - Raw SQLite probes looked promising:
     - months roughly `6.1s -> 3.5s`
     - years/decades around `2s`
   - Full service timings under load got worse/noisy, so this was reverted.
   - Retry first under calm load because it attacks the real expensive part:
     many `COUNT(DISTINCT CASE ...)` aggregates over the full `Play` table.

2. Combine post-page top items and winning attributes.
   - Shape tested:
     - one `song_period_counts` CTE for page period keys.
     - derive top artist/album/song and winning gender/genre/ethnicity/language/country from it.
   - Raw SQL was promising for seasons/years/decades because it replaces two scans
     with one compact aggregate.
   - Full service timings under load became bad, especially seasons, so this was reverted.
   - Retry only after approach 1, and keep debug timing around `top`, `winning`,
     and `maleDays` while testing.

3. Keep explicit `p.play_date IS NOT NULL` in post-page scans.
   - Current safe source change adds this to:
     - `populateTopItems`
     - `populateWinningAttributes`
     - `populateMaleDays`
   - Purpose: make SQLite partial period indexes eligible in those queries.
   - Low-risk because those queries only make sense for dated plays anyway.

4. Re-check expression index usage.
   - Confirm planner uses period indexes for simple grouping:
     - `idx_play_period_month_song`
     - `idx_play_period_season_song`
     - `idx_play_period_year_song`
     - `idx_play_period_decade_song`
   - Use `EXPLAIN QUERY PLAN` before changing SQL further.

## Lower Priority / Probably Avoid

- Extra month index `(SUBSTR(play_date, 1, 7), song_id, play_date)`.
  - Tested as `idx_play_period_month_song_group`.
  - It made months worse and was dropped.

- Forcing indexes with `INDEXED BY`.
  - It showed only minor gains in isolated probes.
  - It makes app SQL depend on exact index names, so avoid unless the win is very clear.

- Normalizing the season expression to one long line.
  - Looked plausible for expression-index matching.
  - Full service timing became much worse, so keep the original multiline expression
    unless a quiet-machine benchmark proves otherwise.

- Play-first grouping before joining `Song`/`Artist`.
  - Shape: group `Play` by `period_key, song_id`, then join metadata.
  - It was inconsistent/slower in probes, so only revisit after the better options.

## Decision Rule

Only keep a change if the second-load HTTP timing improves meaningfully for at
least the affected pages, especially `seasons`, without making another timeframe
page worse. If the win is not clear, revert it.
