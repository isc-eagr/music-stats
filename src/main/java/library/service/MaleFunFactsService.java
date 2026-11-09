package library.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Data-driven ranked lists and charts about male artists for the Playground "Male fun facts" tab. */
@Service
public class MaleFunFactsService {
    private static final int MALE = 2;
    private static final int FEMALE = 1;
    private static final int LIST_SIZE = 10;
    private static final DateTimeFormatter DISPLAY_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final String GENDER = "COALESCE(s.override_gender_id, ar.gender_id)";
    private static final long[] MILESTONE_STEPS = {100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000};
    private static final List<String> WEEKDAYS = List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday");
    private static final List<Integer> WEEKDAY_ORDER = List.of(1, 2, 3, 4, 5, 6, 0);
    /**
     * 13/02/2005 is the robertlover initial Last.fm load (about 9,600 plays stacked into a few minutes), so it is left out
     * of every fact here. Remove this exclusion once that day's play dates are repaired (see AGENTS.md, Known data issues).
     */
    static final String EXCLUDED_DAY = "2005-02-13";
    private static final String NOT_EXCLUDED = " AND p.play_date NOT LIKE '" + EXCLUDED_DAY + "%'";

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    @Autowired
    public MaleFunFactsService(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, Clock.system(ZoneId.of("America/Mexico_City")));
    }

    public MaleFunFactsService(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    /** @param year a listen year, or null for all time */
    public MaleFunFacts getFacts(Integer year) {
        String period = NOT_EXCLUDED + (year == null ? "" : " AND p.play_date >= '" + year + "-01-01' AND p.play_date < '" + (year + 1) + "-01-01'");
        List<MalePlay> plays = jdbcTemplate.query("""
                SELECT p.play_date, s.id AS song_id, s.artist_id, s.album_id, COALESCE(s.length_seconds, 0) AS length_seconds
                FROM Play p
                JOIN Song s ON s.id = p.song_id
                JOIN Artist ar ON ar.id = s.artist_id
                WHERE p.play_date IS NOT NULL AND\s""" + GENDER + " = " + MALE + period + " ORDER BY p.play_date, p.id",
                (rs, row) -> new MalePlay(rs.getString("play_date"), rs.getInt("song_id"), rs.getInt("artist_id"),
                        rs.getObject("album_id") == null ? null : rs.getInt("album_id"), rs.getLong("length_seconds")));
        Map<Integer, ArtistInfo> artists = new HashMap<>();
        jdbcTemplate.query("SELECT id, name, is_band, country, birth_date, death_date FROM Artist", rs -> {
            artists.put(rs.getInt("id"), new ArtistInfo(rs.getString("name"), rs.getInt("is_band") == 1,
                    rs.getString("country"), rs.getString("birth_date"), rs.getString("death_date")));
        });
        Buckets buckets = buckets(year, period);

        Aggregates totals = aggregate(plays);
        Map<Integer, String[]> globalRange = year == null ? totals.ranges : globalArtistRanges();
        Map<Integer, String> songNames = names("Song", totals.songPlays.keySet());
        Map<Integer, String> albumNames = names("Album", totals.albumPlays.keySet());
        Function<Integer, String> artistName = id -> artists.containsKey(id) ? artists.get(id).name() : "Unknown artist";
        Function<Integer, String> artistImage = id -> "/artists/" + id + "/image?thumbnail=true";
        Function<Integer, String> songImage = id -> totals.songAlbum.containsKey(id)
                ? "/albums/" + totals.songAlbum.get(id) + "/image?thumbnail=true" : "/songs/" + id + "/image?thumbnail=true";
        long malePlays = plays.size();

        List<Stat> stats = List.of(
                new Stat("Male plays", formatCount(malePlays), percent(malePlays, buckets.total) + " of all plays"),
                new Stat("Male artists played", formatCount(totals.artistPlays.size()), null),
                new Stat("Male songs played", formatCount(totals.songPlays.size()), null),
                new Stat("Male albums played", formatCount(totals.albumPlays.size()), null),
                new Stat("Time with the men", formatDuration(totals.seconds), null));
        long groupPlays = sum(totals.artistPlays, id -> artists.containsKey(id) && artists.get(id).band());
        GenderSplit split = new GenderSplit(share(malePlays, buckets.total), share(buckets.female, buckets.total),
                share(buckets.total - malePlays - buckets.female, buckets.total), share(malePlays - groupPlays, malePlays),
                formatCount(malePlays - groupPlays), formatCount(groupPlays));

        Function<Integer, String> artistHref = id -> "/artists/" + id, songHref = id -> "/songs/" + id;
        Function<Integer, String> noContext = id -> null;
        Function<Integer, String> songName = id -> songNames.getOrDefault(id, "Unknown song");
        Function<Integer, String> songArtist = id -> artistName.apply(totals.songArtist.get(id));
        Map<String, Long> countryPlays = new HashMap<>();
        Map<String, Set<Integer>> countryArtists = new HashMap<>();
        totals.artistPlays.forEach((id, count) -> {
            String country = artists.containsKey(id) && artists.get(id).country() != null && !artists.get(id).country().isBlank()
                    ? artists.get(id).country() : "Unknown";
            countryPlays.merge(country, count, Long::sum);
            countryArtists.computeIfAbsent(country, key -> new HashSet<>()).add(id);
        });
        List<FactGroup> groups = new ArrayList<>();
        groups.add(new FactGroup("Most played", List.of(
                FactList.ranked("Top male artists", "Plays", rows(top(totals.artistPlays), artistName, artistHref, artistImage,
                        entry -> percent(entry.getValue(), malePlays) + " of male plays", MaleFunFactsService::formatCount)),
                FactList.ranked("Top male songs", "Plays", rows(top(totals.songPlays), songName, songHref, songImage,
                        entry -> songArtist.apply(entry.getKey()), MaleFunFactsService::formatCount)),
                FactList.ranked("Top male albums", "Plays", rows(top(totals.albumPlays), id -> albumNames.getOrDefault(id, "Unknown album"),
                        id -> "/albums/" + id, id -> "/albums/" + id + "/image?thumbnail=true",
                        entry -> artistName.apply(totals.albumArtist.get(entry.getKey())), MaleFunFactsService::formatCount)),
                FactList.ranked("Top countries for men", "Plays", rows(top(countryPlays), Function.identity(), country -> null, country -> null,
                        entry -> formatCount(countryArtists.get(entry.getKey()).size()) + " artists", MaleFunFactsService::formatCount)))));

        // Days, weeks and months the period covers, from its first recorded play to today (or the end of the selected year).
        LocalDate today = LocalDate.now(clock);
        String firstPlay = jdbcTemplate.queryForObject("SELECT MIN(p.play_date) FROM Play p WHERE p.play_date IS NOT NULL" + period, String.class);
        LocalDate from = firstPlay == null ? today : LocalDate.parse(firstPlay.substring(0, 10));
        LocalDate to = year == null || year >= today.getYear() ? today : LocalDate.of(year, 12, 31);
        long days = Math.max(1, ChronoUnit.DAYS.between(from, to) + 1);
        long weeks = Math.max(1, ChronoUnit.WEEKS.between(from.with(DayOfWeek.MONDAY), to.with(DayOfWeek.MONDAY)) + 1);
        long months = Math.max(1, ChronoUnit.MONTHS.between(from.withDayOfMonth(1), to.withDayOfMonth(1)) + 1);
        Map<Integer, Long> streakDays = totals.bestStreaks.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().days()));
        groups.add(new FactGroup("Always around", List.of(
                FactList.ranked("Most days with a play", "Days", rows(top(totals.artistDays.active), artistName, artistHref, artistImage,
                        entry -> percent(entry.getValue(), days) + " of " + formatCount(days) + " days", MaleFunFactsService::formatCount)),
                FactList.ranked("Most weeks with a play", "Weeks", rows(top(totals.artistWeeks.active), artistName, artistHref, artistImage,
                        entry -> percent(entry.getValue(), weeks) + " of " + formatCount(weeks) + " weeks", MaleFunFactsService::formatCount)),
                FactList.ranked("Most months with a play", "Months", rows(top(totals.artistMonths.active), artistName, artistHref, artistImage,
                        entry -> percent(entry.getValue(), months) + " of " + formatCount(months) + " months", MaleFunFactsService::formatCount)),
                FactList.ranked("Longest daily streaks", "Days in a row", rows(top(streakDays), artistName, artistHref, artistImage,
                        entry -> display(totals.bestStreaks.get(entry.getKey()).start()) + " to " + display(totals.bestStreaks.get(entry.getKey()).end()),
                        MaleFunFactsService::formatCount)),
                spans(totals, artistName, artistImage))));

        groups.add(new FactGroup("Binges", List.of(
                peaks("Biggest single days", "Plays that day", totals.artistDays, artistName, artistHref, artistImage, noContext, MaleFunFactsService::display),
                peaks("Biggest single weeks", "Plays that week", totals.artistWeeks, artistName, artistHref, artistImage, noContext, MaleFunFactsService::weekOf),
                peaks("Biggest single months", "Plays that month", totals.artistMonths, artistName, artistHref, artistImage, noContext, MaleFunFactsService::monthOf),
                peaks("Song obsessions: one day", "Plays that day", totals.songDays, songName, songHref, songImage, songArtist, MaleFunFactsService::display),
                peaks("Song obsessions: one week", "Plays that week", totals.songWeeks, songName, songHref, songImage, songArtist, MaleFunFactsService::weekOf))));

        Map<Integer, Long> catalogSizes = totals.artistSongs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (long) entry.getValue().size()));
        groups.add(new FactGroup("Catalog depth", List.of(
                FactList.ranked("Deepest catalogs", "Songs played", rows(top(catalogSizes), artistName, artistHref, artistImage,
                        entry -> formatCount(totals.artistPlays.get(entry.getKey())) + " plays", MaleFunFactsService::formatCount)),
                FactList.ranked("One-song men", "Plays", rows(top(filter(totals.artistPlays, id -> totals.artistSongs.get(id).size() == 1)),
                        artistName, artistHref, artistImage,
                        entry -> songName.apply(totals.artistSongs.get(entry.getKey()).iterator().next()), MaleFunFactsService::formatCount)))));

        List<FactList> history = new ArrayList<>();
        String cutoff = today.minusYears(3).toString();
        history.add(FactList.ranked("Not played in 3+ years", "Plays", rows(top(filter(totals.artistPlays,
                        id -> globalRange.containsKey(id) && globalRange.get(id)[1].compareTo(cutoff) < 0)), artistName, artistHref, artistImage,
                entry -> "Last played " + displayDay(globalRange.get(entry.getKey())[1]), MaleFunFactsService::formatCount)));
        if (year != null) {
            String yearStart = year + "-01-01";
            history.add(FactList.ranked("First heard in " + year, "Plays", rows(top(filter(totals.artistPlays,
                            id -> globalRange.containsKey(id) && globalRange.get(id)[0].compareTo(yearStart) >= 0)), artistName, artistHref, artistImage,
                    entry -> "First play " + displayDay(globalRange.get(entry.getKey())[0]), MaleFunFactsService::formatCount)));
        }
        history.add(FactList.ranked("Men who have passed", "Plays", rows(top(filter(totals.artistPlays,
                        id -> artists.containsKey(id) && hasDate(artists.get(id).deathDate()))), artistName, artistHref, artistImage,
                entry -> "Died " + displayDay(artists.get(entry.getKey()).deathDate()), MaleFunFactsService::formatCount)));
        String monthDay = today.toString().substring(5);
        history.add(FactList.ranked("Born on " + today.format(DateTimeFormatter.ofPattern("dd/MM")), "Plays", rows(top(filter(totals.artistPlays,
                        id -> artists.containsKey(id) && hasDate(artists.get(id).birthDate()) && artists.get(id).birthDate().substring(5, 10).equals(monthDay))),
                artistName, artistHref, artistImage,
                entry -> "Born " + displayDay(artists.get(entry.getKey()).birthDate()), MaleFunFactsService::formatCount)));
        history.add(milestones(plays, songNames, artistName, songImage));
        groups.add(new FactGroup("Then and now", history));

        return new MaleFunFacts(year, stats, split, groups, year == null ? "Male plays by year" : "Male plays by month",
                buckets.timeline, buckets.hours, buckets.weekdays, buckets.heatmap, buckets.monthsOfYear,
                chartFacts(year, artistName, artistImage, songImage));
    }

    private Buckets buckets(Integer year, String period) {
        TreeMap<String, long[]> timeline = new TreeMap<>();
        long[][][] cells = new long[7][24][3];
        long[][] months = new long[12][3];
        jdbcTemplate.query("SELECT strftime('%Y-%m', p.play_date) AS bucket, "
                + "CAST(strftime('%w', p.play_date) AS INTEGER) AS weekday, CAST(strftime('%H', p.play_date) AS INTEGER) AS hour, COUNT(*) AS total, "
                + "SUM(CASE WHEN " + GENDER + " = " + MALE + " THEN 1 ELSE 0 END) AS male, "
                + "SUM(CASE WHEN " + GENDER + " = " + FEMALE + " THEN 1 ELSE 0 END) AS female "
                + "FROM Play p LEFT JOIN Song s ON s.id = p.song_id LEFT JOIN Artist ar ON ar.id = s.artist_id "
                + "WHERE strftime('%H', p.play_date) IS NOT NULL" + period + " GROUP BY bucket, weekday, hour", rs -> {
            long[] values = {rs.getLong("total"), rs.getLong("male"), rs.getLong("female")};
            String bucket = rs.getString("bucket");
            long[] slot = timeline.computeIfAbsent(year == null ? bucket.substring(0, 4) : bucket.substring(5, 7), key -> new long[3]);
            long[] month = months[Integer.parseInt(bucket.substring(5, 7)) - 1];
            long[] cell = cells[rs.getInt("weekday")][rs.getInt("hour")];
            for (int i = 0; i < 3; i++) { slot[i] += values[i]; cell[i] += values[i]; month[i] += values[i]; }
        });
        Buckets result = new Buckets();
        // Month of year only adds something over the monthly timeline when looking at all time.
        if (year == null) {
            double low = 100, high = 0;
            for (long[] month : months) if (month[0] > 0) { low = Math.min(low, share(month[1], month[0])); high = Math.max(high, share(month[1], month[0])); }
            for (int month = 0; month < 12; month++) {
                double value = share(months[month][1], months[month][0]);
                result.monthsOfYear.add(new MonthTile(Month.of(month + 1).getDisplayName(TextStyle.FULL, Locale.ENGLISH), round(value),
                        formatCount(months[month][0]), months[month][0] == 0 ? 0 : level(value, low, high)));
            }
        }
        timeline.values().forEach(values -> { result.total += values[0]; result.female += values[2]; });

        List<String> keys = new ArrayList<>();
        if (year != null) for (int month = 1; month <= 12; month++) keys.add(String.format("%02d", month));
        else if (!timeline.isEmpty()) {
            for (int value = Integer.parseInt(timeline.firstKey()); value <= Integer.parseInt(timeline.lastKey()); value++) keys.add(String.valueOf(value));
        }
        long timelineMax = timeline.values().stream().mapToLong(values -> values[0]).max().orElse(0);
        for (String key : keys) {
            long[] values = timeline.getOrDefault(key, new long[3]);
            String label = year == null ? key : Month.of(Integer.parseInt(key)).getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
            result.timeline.add(bar(label, values[1], values[0], timelineMax));
        }
        long[][] hours = new long[24][2], weekdays = new long[7][2];
        for (int day = 0; day < 7; day++) for (int hour = 0; hour < 24; hour++) {
            hours[hour][0] += cells[day][hour][0]; hours[hour][1] += cells[day][hour][1];
            weekdays[day][0] += cells[day][hour][0]; weekdays[day][1] += cells[day][hour][1];
        }
        long hourMax = 0, weekdayMax = 0;
        for (long[] hour : hours) hourMax = Math.max(hourMax, hour[0]);
        for (long[] day : weekdays) weekdayMax = Math.max(weekdayMax, day[0]);
        for (int hour = 0; hour < 24; hour++) result.hours.add(bar(String.format("%02d:00", hour), hours[hour][1], hours[hour][0], hourMax));
        for (int day : WEEKDAY_ORDER) result.weekdays.add(bar(WEEKDAYS.get(day), weekdays[day][1], weekdays[day][0], weekdayMax));

        // Heatmap shades compare cells against each other; cells under a tenth of the average volume stay muted.
        double minVolume = Math.max(1, result.total / 168.0 / 10);
        double low = 100, high = 0;
        for (long[][] day : cells) for (long[] cell : day) if (cell[0] >= minVolume) {
            low = Math.min(low, share(cell[1], cell[0])); high = Math.max(high, share(cell[1], cell[0]));
        }
        for (int day : WEEKDAY_ORDER) {
            List<HeatCell> row = new ArrayList<>();
            for (int hour = 0; hour < 24; hour++) {
                long[] cell = cells[day][hour];
                double value = share(cell[1], cell[0]);
                int level = cell[0] < minVolume ? 0 : level(value, low, high);
                row.add(new HeatCell(WEEKDAYS.get(day) + " " + String.format("%02d:00", hour) + ": " + String.format("%.1f", value)
                        + "% male of " + formatCount(cell[0]) + " plays", level));
            }
            result.heatmap.add(new HeatRow(WEEKDAYS.get(day).substring(0, 3), row));
        }
        return result;
    }

    private FactList spans(Aggregates totals, Function<Integer, String> artistName, Function<Integer, String> artistImage) {
        List<Map.Entry<Integer, String[]>> ranked = totals.ranges.entrySet().stream()
                .filter(entry -> totals.artistPlays.get(entry.getKey()) >= 10)
                .sorted(Comparator.comparingLong((Map.Entry<Integer, String[]> entry) -> spanDays(entry.getValue())).reversed()
                        .thenComparing(entry -> -totals.artistPlays.get(entry.getKey())))
                .limit(LIST_SIZE).toList();
        String first = totals.ranges.values().stream().map(range -> range[0]).min(String::compareTo).orElse(null);
        String last = totals.ranges.values().stream().map(range -> range[1]).max(String::compareTo).orElse(null);
        double axis = first == null ? 1 : Math.max(1, spanDays(new String[] {first, last}));
        List<FactRow> rows = ranked.stream().map(entry -> {
            String[] range = entry.getValue();
            double start = spanDays(new String[] {first, range[0]}) / axis * 100;
            return new FactRow(artistName.apply(entry.getKey()), "/artists/" + entry.getKey(), artistImage.apply(entry.getKey()),
                    displayDay(range[0]) + " to " + displayDay(range[1]) + " · " + formatCount(totals.artistPlays.get(entry.getKey())) + " plays",
                    String.format("%.1f", spanDays(range) / 365.25), round(start), round(Math.max(1, spanDays(range) / axis * 100)));
        }).toList();
        return new FactList("Longest time listening", "Years", "spans",
                first == null ? null : first.substring(0, 4), last == null ? null : last.substring(0, 4), rows);
    }

    private FactList milestones(List<MalePlay> plays, Map<Integer, String> songNames, Function<Integer, String> artistName,
                                Function<Integer, String> songImage) {
        long total = plays.size();
        long step = MILESTONE_STEPS[MILESTONE_STEPS.length - 1];
        for (long candidate : MILESTONE_STEPS) if (total / candidate < LIST_SIZE - 1) { step = candidate; break; }
        List<FactRow> rows = new ArrayList<>();
        for (long number = 1; number <= total && rows.size() < LIST_SIZE; number = number == 1 ? step : number + step) {
            MalePlay play = plays.get((int) number - 1);
            rows.add(new FactRow("Male play #" + formatCount(number), "/songs/" + play.songId(), songImage.apply(play.songId()),
                    songNames.getOrDefault(play.songId(), "Unknown song") + " · " + artistName.apply(play.artistId()), displayDay(play.date()), 0, 0));
        }
        if (total > 0) {
            long next = (total / step + 1) * step;
            rows.add(new FactRow("Male play #" + formatCount(next), null, null, formatCount(next - total) + " plays to go", "Next", 0, 0));
        }
        return new FactList("Milestones", "Date", "milestones", null, null, rows);
    }

    private Aggregates aggregate(List<MalePlay> plays) {
        Aggregates result = new Aggregates();
        Map<Integer, Streak> currentStreaks = new HashMap<>();
        for (MalePlay play : plays) {
            result.seconds += play.lengthSeconds();
            result.artistPlays.merge(play.artistId(), 1L, Long::sum);
            result.songPlays.merge(play.songId(), 1L, Long::sum);
            result.songArtist.putIfAbsent(play.songId(), play.artistId());
            result.artistSongs.computeIfAbsent(play.artistId(), key -> new HashSet<>()).add(play.songId());
            if (play.albumId() != null) {
                result.albumPlays.merge(play.albumId(), 1L, Long::sum);
                result.albumArtist.putIfAbsent(play.albumId(), play.artistId());
                result.songAlbum.putIfAbsent(play.songId(), play.albumId());
            }
            result.ranges.computeIfAbsent(play.artistId(), key -> new String[] {play.date(), play.date()})[1] = play.date();
            LocalDate date = LocalDate.parse(play.date().substring(0, 10));
            LocalDate week = date.with(DayOfWeek.MONDAY);
            result.artistDays.add(play.artistId(), date);
            result.artistWeeks.add(play.artistId(), week);
            result.artistMonths.add(play.artistId(), date.withDayOfMonth(1));
            result.songDays.add(play.songId(), date);
            result.songWeeks.add(play.songId(), week);
            Streak streak = currentStreaks.get(play.artistId());
            if (streak == null || date.isAfter(streak.end().plusDays(1))) streak = new Streak(date, date, 1);
            else if (date.equals(streak.end().plusDays(1))) streak = new Streak(streak.start(), date, streak.days() + 1);
            currentStreaks.put(play.artistId(), streak);
            Streak longest = result.bestStreaks.get(play.artistId());
            if (longest == null || streak.days() > longest.days()) result.bestStreaks.put(play.artistId(), streak);
        }
        return result;
    }

    /** Each id's busiest bucket, with the optional context (such as a song's artist) before the bucket's date. */
    private static FactList peaks(String title, String valueLabel, Peaks peaks, Function<Integer, String> name, Function<Integer, String> href,
                                  Function<Integer, String> image, Function<Integer, String> context, Function<LocalDate, String> when) {
        Map<Integer, Long> plays = peaks.best.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()[0]));
        return FactList.ranked(title, valueLabel, rows(top(plays), name, href, image, entry -> {
            String date = when.apply(LocalDate.ofEpochDay(peaks.best.get(entry.getKey())[1]));
            String prefix = context.apply(entry.getKey());
            return prefix == null ? date : prefix + " · " + date;
        }, MaleFunFactsService::formatCount));
    }

    /** Male facts from the stored weekly charts (top 20 songs, top 10 albums), skipping the chart week that covers EXCLUDED_DAY. */
    private ChartFacts chartFacts(Integer year, Function<Integer, String> artistName, Function<Integer, String> artistImage,
                                  Function<Integer, String> songImage) {
        String filter = " AND NOT (c.period_start_date <= '" + EXCLUDED_DAY + "' AND c.period_end_date >= '" + EXCLUDED_DAY + "')"
                + (year == null ? "" : " AND c.period_start_date >= '" + year + "-01-01' AND c.period_start_date < '" + (year + 1) + "-01-01'");
        List<ChartRow> songRows = jdbcTemplate.query("""
                SELECT c.period_start_date, c.period_end_date, e.position, e.song_id AS item_id, s.artist_id,
                       COALESCE(s.override_gender_id, ar.gender_id) AS gender
                FROM ChartEntry e
                JOIN Chart c ON c.id = e.chart_id
                JOIN Song s ON s.id = e.song_id
                JOIN Artist ar ON ar.id = s.artist_id
                WHERE c.period_type = 'weekly' AND c.chart_type = 'song'""" + filter + " ORDER BY c.period_start_date, e.position",
                MaleFunFactsService::chartRow);
        List<ChartRow> albumRows = jdbcTemplate.query("""
                SELECT c.period_start_date, c.period_end_date, e.position, e.album_id AS item_id, al.artist_id, ar.gender_id AS gender
                FROM ChartEntry e
                JOIN Chart c ON c.id = e.chart_id
                JOIN Album al ON al.id = e.album_id
                JOIN Artist ar ON ar.id = al.artist_id
                WHERE c.period_type = 'weekly' AND c.chart_type = 'album'""" + filter + " ORDER BY c.period_start_date, e.position",
                MaleFunFactsService::chartRow);
        List<ChartRow> maleSongs = songRows.stream().filter(row -> row.gender() == MALE).toList();
        List<ChartRow> maleAlbums = albumRows.stream().filter(row -> row.gender() == MALE).toList();
        Map<Integer, String> songNames = names("Song", maleSongs.stream().map(ChartRow::itemId).collect(Collectors.toSet()));
        Map<Integer, String> albumNames = names("Album", maleAlbums.stream().map(ChartRow::itemId).collect(Collectors.toSet()));
        Map<Integer, Integer> songArtist = new HashMap<>(), albumArtist = new HashMap<>();
        maleSongs.forEach(row -> songArtist.put(row.itemId(), row.artistId()));
        maleAlbums.forEach(row -> albumArtist.put(row.itemId(), row.artistId()));
        Function<Integer, String> songName = id -> songNames.getOrDefault(id, "Unknown song");
        Function<Integer, String> albumName = id -> albumNames.getOrDefault(id, "Unknown album");

        Map<Integer, Long> songNumberOnes = count(maleSongs, row -> row.position() == 1, ChartRow::itemId);
        Map<Integer, Long> artistNumberOnes = count(maleSongs, row -> row.position() == 1, ChartRow::artistId);
        Map<Integer, Long> songWeeks = count(maleSongs, row -> true, ChartRow::itemId);
        Map<Integer, Long> albumNumberOnes = count(maleAlbums, row -> row.position() == 1, ChartRow::itemId);
        Map<Integer, Long> albumWeeks = count(maleAlbums, row -> true, ChartRow::itemId);
        Map<Integer, Integer> peaks = new HashMap<>();
        maleSongs.forEach(row -> peaks.merge(row.itemId(), row.position(), Math::min));
        Map<Integer, Set<Integer>> artistSongs = new HashMap<>();
        maleSongs.forEach(row -> artistSongs.computeIfAbsent(row.artistId(), key -> new HashSet<>()).add(row.itemId()));
        Map<Integer, Long> artistChartWeeks = count(maleSongs, row -> true, ChartRow::artistId);

        // A #1 run continues while the same song tops consecutive chart weeks, whoever held the other weeks.
        Map<Integer, String[]> bestRuns = new HashMap<>();
        Map<Integer, Long> runLengths = new HashMap<>();
        Integer runSong = null;
        String runStart = null;
        long runLength = 0;
        for (ChartRow row : songRows) {
            if (row.position() != 1) continue;
            if (Integer.valueOf(row.itemId()).equals(runSong)) runLength++;
            else { runSong = row.itemId(); runStart = row.start(); runLength = 1; }
            if (row.gender() == MALE && runLength > runLengths.getOrDefault(row.itemId(), 0L)) {
                runLengths.put(row.itemId(), runLength);
                bestRuns.put(row.itemId(), new String[] {runStart, row.end()});
            }
        }

        long numberOneWeeks = songRows.stream().filter(row -> row.position() == 1).count();
        long albumNumberOneWeeks = albumRows.stream().filter(row -> row.position() == 1).count();
        long maleNumberOneWeeks = songNumberOnes.values().stream().mapToLong(Long::longValue).sum();
        long maleAlbumNumberOneWeeks = albumNumberOnes.values().stream().mapToLong(Long::longValue).sum();
        List<Stat> stats = List.of(
                new Stat("Weeks with a male #1 song", formatCount(maleNumberOneWeeks), percent(maleNumberOneWeeks, numberOneWeeks) + " of " + formatCount(numberOneWeeks) + " weeks"),
                new Stat("Weeks with a male #1 album", formatCount(maleAlbumNumberOneWeeks), percent(maleAlbumNumberOneWeeks, albumNumberOneWeeks) + " of " + formatCount(albumNumberOneWeeks) + " weeks"),
                new Stat("Male songs that hit #1", formatCount(songNumberOnes.size()), formatCount(songWeeks.size()) + " male songs charted"),
                new Stat("Male albums that hit #1", formatCount(albumNumberOnes.size()), formatCount(albumWeeks.size()) + " male albums charted"));
        Function<Integer, String> songHref = id -> "/songs/" + id, albumHref = id -> "/albums/" + id, artistHref = id -> "/artists/" + id;
        Function<Integer, String> albumImage = id -> "/albums/" + id + "/image?thumbnail=true";
        List<FactList> lists = List.of(
                FactList.ranked("Most weeks at #1: songs", "Weeks", rows(top(songNumberOnes), songName, songHref, songImage,
                        entry -> artistName.apply(songArtist.get(entry.getKey())), MaleFunFactsService::formatCount)),
                FactList.ranked("Most weeks at #1: artists", "Weeks", rows(top(artistNumberOnes), artistName, artistHref, artistImage,
                        entry -> formatCount(maleSongs.stream().filter(row -> row.position() == 1 && row.artistId() == entry.getKey())
                                .map(ChartRow::itemId).distinct().count()) + " different #1 songs", MaleFunFactsService::formatCount)),
                FactList.ranked("Longest #1 runs", "Weeks in a row", rows(top(runLengths), songName, songHref, songImage,
                        entry -> artistName.apply(songArtist.get(entry.getKey())) + " · " + displayDay(bestRuns.get(entry.getKey())[0])
                                + " to " + displayDay(bestRuns.get(entry.getKey())[1]), MaleFunFactsService::formatCount)),
                FactList.ranked("Most weeks on the song chart", "Weeks", rows(top(songWeeks), songName, songHref, songImage,
                        entry -> artistName.apply(songArtist.get(entry.getKey())) + " · peak #" + peaks.get(entry.getKey()), MaleFunFactsService::formatCount)),
                FactList.ranked("Most charted songs: artists", "Songs", rows(top(artistSongs.entrySet().stream()
                                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (long) entry.getValue().size()))), artistName, artistHref, artistImage,
                        entry -> formatCount(artistChartWeeks.get(entry.getKey())) + " chart weeks", MaleFunFactsService::formatCount)),
                FactList.ranked("Most weeks at #1: albums", "Weeks", rows(top(albumNumberOnes), albumName, albumHref, albumImage,
                        entry -> artistName.apply(albumArtist.get(entry.getKey())), MaleFunFactsService::formatCount)),
                FactList.ranked("Most weeks on the album chart", "Weeks", rows(top(albumWeeks), albumName, albumHref, albumImage,
                        entry -> artistName.apply(albumArtist.get(entry.getKey())), MaleFunFactsService::formatCount)));
        return new ChartFacts(stats, lists);
    }

    private static ChartRow chartRow(ResultSet rs, int row) throws SQLException {
        return new ChartRow(rs.getString("period_start_date"), rs.getString("period_end_date"), rs.getInt("position"),
                rs.getInt("item_id"), rs.getInt("artist_id"), rs.getInt("gender"));
    }

    private static Map<Integer, Long> count(List<ChartRow> rows, Predicate<ChartRow> keep, Function<ChartRow, Integer> key) {
        return rows.stream().filter(keep).collect(Collectors.groupingBy(key, Collectors.counting()));
    }

    private Map<Integer, String[]> globalArtistRanges() {
        Map<Integer, String[]> ranges = new HashMap<>();
        jdbcTemplate.query("""
                SELECT s.artist_id, MIN(p.play_date) AS first_play, MAX(p.play_date) AS last_play
                FROM Play p
                JOIN Song s ON s.id = p.song_id
                JOIN Artist ar ON ar.id = s.artist_id
                WHERE p.play_date IS NOT NULL AND\s""" + GENDER + " = " + MALE + NOT_EXCLUDED + " GROUP BY s.artist_id", rs -> {
            ranges.put(rs.getInt("artist_id"), new String[] {rs.getString("first_play"), rs.getString("last_play")});
        });
        return ranges;
    }

    private Map<Integer, String> names(String table, Collection<Integer> ids) {
        Map<Integer, String> names = new HashMap<>();
        List<Integer> list = new ArrayList<>(ids);
        for (int start = 0; start < list.size(); start += 500) {
            List<Integer> chunk = list.subList(start, Math.min(list.size(), start + 500));
            String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
            jdbcTemplate.query("SELECT id, name FROM " + table + " WHERE id IN (" + placeholders + ")",
                    rs -> { names.put(rs.getInt("id"), rs.getString("name")); }, chunk.toArray());
        }
        return names;
    }

    /** Ranked rows whose bar width is relative to the first (largest) value. */
    private static <K> List<FactRow> rows(List<Map.Entry<K, Long>> entries, Function<K, String> name, Function<K, String> href,
                                          Function<K, String> image, Function<Map.Entry<K, Long>, String> detail, Function<Long, String> value) {
        double max = entries.isEmpty() ? 1 : Math.max(1, entries.getFirst().getValue());
        return entries.stream().map(entry -> new FactRow(name.apply(entry.getKey()), href.apply(entry.getKey()), image.apply(entry.getKey()),
                detail.apply(entry), value.apply(entry.getValue()), 0, round(entry.getValue() / max * 100))).toList();
    }

    private static <K> List<Map.Entry<K, Long>> top(Map<K, Long> counts) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<K, Long>comparingByValue().reversed().thenComparing(entry -> String.valueOf(entry.getKey())))
                .limit(LIST_SIZE).toList();
    }

    private static Map<Integer, Long> filter(Map<Integer, Long> counts, Predicate<Integer> keep) {
        return counts.entrySet().stream().filter(entry -> keep.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static long sum(Map<Integer, Long> counts, Predicate<Integer> keep) {
        return counts.entrySet().stream().filter(entry -> keep.test(entry.getKey())).mapToLong(Map.Entry::getValue).sum();
    }

    private static long spanDays(String[] range) {
        return ChronoUnit.DAYS.between(LocalDate.parse(range[0].substring(0, 10)), LocalDate.parse(range[1].substring(0, 10)));
    }

    private static ShareBar bar(String label, long male, long total, long max) {
        return new ShareBar(label, male, total, round(share(male, total)), round(max == 0 ? 0 : total * 100.0 / max));
    }

    /** Shade 1-5 for a male share relative to the lowest and highest shares in the same chart. */
    private static int level(double value, double low, double high) {
        return high <= low ? 3 : 1 + (int) Math.min(4, Math.max(0, Math.floor((value - low) / (high - low) * 5)));
    }

    private static double share(long part, long total) { return total == 0 ? 0 : part * 100.0 / total; }
    private static double round(double value) { return Math.round(value * 10) / 10.0; }
    private static boolean hasDate(String value) { return value != null && value.length() >= 10; }
    private static String display(LocalDate date) { return date.format(DISPLAY_DATE); }
    private static String weekOf(LocalDate monday) { return "week of " + display(monday); }
    private static String monthOf(LocalDate first) { return first.format(DateTimeFormatter.ofPattern("MMMM yyyy", Locale.ENGLISH)); }
    private static String displayDay(String value) { return display(LocalDate.parse(value.substring(0, 10))); }
    private static String formatCount(long value) { return String.format("%,d", value); }
    private static String percent(long part, long total) { return String.format("%.1f%%", share(part, total)); }

    static String formatDuration(long seconds) {
        if (seconds >= 86400) return (seconds / 86400) + "d " + (seconds % 86400 / 3600) + "h";
        if (seconds >= 3600) return (seconds / 3600) + "h " + (seconds % 3600 / 60) + "m";
        return (seconds / 60) + "m";
    }

    private record MalePlay(String date, int songId, int artistId, Integer albumId, long lengthSeconds) {}
    private record ChartRow(String start, String end, int position, int itemId, int artistId, int gender) {}
    private record ArtistInfo(String name, boolean band, String country, String birthDate, String deathDate) {}
    private record Streak(LocalDate start, LocalDate end, long days) {}

    /** Play counts per (id, day/week/month bucket), keeping each id's busiest bucket as {plays, epochDay}. */
    private static final class Peaks {
        final Map<Long, Long> counts = new HashMap<>();
        final Map<Integer, long[]> best = new HashMap<>();
        /** How many distinct buckets each id has at least one play in. */
        final Map<Integer, Long> active = new HashMap<>();

        void add(int id, LocalDate bucket) {
            long plays = counts.merge(id * 100_000L + bucket.toEpochDay(), 1L, Long::sum);
            if (plays == 1) active.merge(id, 1L, Long::sum);
            long[] current = best.get(id);
            if (current == null || plays > current[0]) best.put(id, new long[] {plays, bucket.toEpochDay()});
        }
    }

    private static final class Aggregates {
        long seconds;
        final Map<Integer, Long> artistPlays = new HashMap<>();
        final Map<Integer, Long> songPlays = new HashMap<>();
        final Map<Integer, Long> albumPlays = new HashMap<>();
        final Map<Integer, Integer> songArtist = new HashMap<>();
        final Map<Integer, Integer> albumArtist = new HashMap<>();
        final Map<Integer, Integer> songAlbum = new HashMap<>();
        final Map<Integer, Set<Integer>> artistSongs = new HashMap<>();
        final Map<Integer, String[]> ranges = new HashMap<>();
        final Map<Integer, Streak> bestStreaks = new HashMap<>();
        final Peaks artistDays = new Peaks(), artistWeeks = new Peaks(), artistMonths = new Peaks();
        final Peaks songDays = new Peaks(), songWeeks = new Peaks();
    }

    private static final class Buckets {
        long total, female;
        final List<ShareBar> timeline = new ArrayList<>();
        final List<ShareBar> hours = new ArrayList<>();
        final List<ShareBar> weekdays = new ArrayList<>();
        final List<HeatRow> heatmap = new ArrayList<>();
        final List<MonthTile> monthsOfYear = new ArrayList<>();
    }

    public record Stat(String label, String value, String detail) {}
    /** barStart and barWidth are percentages: ranked rows start at 0, spans sit on a shared time axis. */
    public record FactRow(String name, String href, String imageUrl, String detail, String value, double barStart, double barWidth) {}
    public record FactList(String title, String valueLabel, String style, String axisStart, String axisEnd, List<FactRow> rows) {
        static FactList ranked(String title, String valueLabel, List<FactRow> rows) { return new FactList(title, valueLabel, "ranked", null, null, rows); }
    }
    /** share is the male percentage; height is total volume relative to the busiest bar in the same chart. */
    public record ShareBar(String label, long male, long total, double share, double height) {}
    public record HeatCell(String label, int level) {}
    public record HeatRow(String label, List<HeatCell> cells) {}
    public record MonthTile(String label, double share, String plays, int level) {}
    public record GenderSplit(double male, double female, double other, double solo, String soloPlays, String groupPlays) {}
    public record FactGroup(String title, List<FactList> lists) {}
    public record ChartFacts(List<Stat> stats, List<FactList> lists) {}
    public record MaleFunFacts(Integer year, List<Stat> stats, GenderSplit split, List<FactGroup> groups, String timelineTitle,
                               List<ShareBar> timeline, List<ShareBar> hours, List<ShareBar> weekdays, List<HeatRow> heatmap,
                               List<MonthTile> monthsOfYear, ChartFacts charts) {}
}
