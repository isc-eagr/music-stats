package library.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Backs the Seasonal / Yearly overview.
 * <ul>
 *   <li>Songs and Albums: the Season wall, one row per year with a tile per season (Winter, Spring,
 *   Summer, Fall) plus the Year chart, each showing that chart's #1 and one dot per position.</li>
 *   <li>Artists: one board with every artist that appears on a seasonal or yearly song chart.</li>
 * </ul>
 * Seasonal song positions 31-35 are bubbling-under extras: they get their own dots and column but
 * never count as chart entries, points or leaders.
 */
@Service
public class SeasonalWallService {

    public static final List<String> SEASONS = List.of("Winter", "Spring", "Summer", "Fall");
    static final int CHARTED_LIMIT = ChartService.SEASONAL_YEARLY_SONGS_COUNT;

    private final JdbcTemplate jdbcTemplate;

    public SeasonalWallService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** {@code state}: "chart" (has entries), "empty" (chart saved without entries; a draft is in progress), "none" (no chart), "later" (not reached yet). */
    public record Tile(String state, String periodType, String periodKey, String label,
                       String title, String subtitle, String link, String coverUrl, String gender,
                       Integer position, List<String> dots, int maleCount, int femaleCount, int chartedCount,
                       boolean draft, boolean crowned) {

        public String chartLink() {
            return periodKey == null ? null : "/charts/" + periodType + "/" + periodKey;
        }

        public String initials() {
            if (title == null || title.isBlank()) return "";
            StringBuilder out = new StringBuilder();
            for (String word : title.trim().split("\\s+")) {
                if (!word.isEmpty() && Character.isLetterOrDigit(word.charAt(0))) out.append(Character.toUpperCase(word.charAt(0)));
                if (out.length() == 2) break;
            }
            return out.toString();
        }

        Tile crown() {
            return new Tile(state, periodType, periodKey, label, title, subtitle, link, coverUrl, gender,
                    position, dots, maleCount, femaleCount, chartedCount, draft, true);
        }
    }

    public record Row(int year, List<Tile> seasons, Tile yearTile) {
    }

    public record GenderCounts(int male, int female, int other) {
        public int total() {
            return male + female + other;
        }
    }

    public record Summary(int seasonsCharted, int seasonsWaiting, String firstSeason, String lastSeason,
                          GenderCounts seasonLeaders, GenderCounts yearLeaders,
                          int seasonLeadersOnYearly, int seasonLeadersInYearlyTop10) {
    }

    public record Wall(String tab, List<Row> rows, Summary summary) {
    }

    /**
     * One artist across every seasonal and yearly song chart. Points are 31 minus the position
     * (a #1 is worth 30). First / last season are their seasonal top-30 span; the order fields sort them.
     */
    public record ArtistRow(int artistId, String name, String gender, String coverUrl,
                            int seasonPoints, int seasonEntries, int seasonNumberOnes, int seasonTop10, int bubblingUnder,
                            int yearPoints, int yearEntries, int yearNumberOnes, int yearTop10,
                            String firstSeason, int firstSeasonOrder, String lastSeason, int lastSeasonOrder) {

        public String link() {
            return "/artists/" + artistId;
        }
    }

    /** One listed entry; {@code gender} is the item's own (song override aware), {@code artistGender} the artist's. */
    record Entry(String periodType, String periodKey, boolean finalized, Integer position,
                 Integer itemId, String itemName, Integer artistId, String artistName,
                 String gender, String artistGender, String coverUrl, String artistCoverUrl) {
    }

    record ChartKey(String periodType, String periodKey) {
    }

    public Wall build(String tab) {
        String normalized = "album".equals(tab) ? "album" : "song";
        Map<ChartKey, List<Entry>> charts = loadCharts(normalized);
        Map<ChartKey, Tile> tiles = new HashMap<>();
        charts.forEach((key, entries) -> tiles.put(key, tile(key, entries, normalized)));

        TreeSet<Integer> years = new TreeSet<>(Comparator.reverseOrder());
        charts.keySet().forEach(key -> {
            Integer year = yearOf(key.periodKey());
            if (year != null) years.add(year);
        });
        int latestOrder = charts.keySet().stream()
                .filter(key -> "seasonal".equals(key.periodType()) && seasonIndex(key.periodKey()) >= 0)
                .mapToInt(key -> seasonOrder(key.periodKey()))
                .max().orElse(Integer.MIN_VALUE);

        List<Row> rows = new ArrayList<>();
        for (int year : years) {
            Tile yearTile = tiles.getOrDefault(new ChartKey("yearly", String.valueOf(year)),
                    missing("yearly", String.valueOf(year), false));
            List<Tile> seasons = new ArrayList<>();
            for (String season : SEASONS) {
                String key = year + "-" + season;
                Tile tile = tiles.get(new ChartKey("seasonal", key));
                if (tile == null) {
                    tile = missing("seasonal", season + " " + year, seasonOrder(key) > latestOrder);
                } else if ("chart".equals(tile.state()) && "chart".equals(yearTile.state())
                        && tile.link() != null && tile.link().equals(yearTile.link())) {
                    tile = tile.crown();
                }
                seasons.add(tile);
            }
            rows.add(new Row(year, seasons, yearTile));
        }
        return new Wall(normalized, rows, summary(charts, tiles, normalized));
    }

    /**
     * Every artist on a seasonal or yearly song chart (bubbling-under only included), seasonal points first.
     * With {@code includeFeatured}, a song also counts for each featured artist (same rule as the chart overview tables).
     */
    public List<ArtistRow> artistBoard(boolean includeFeatured) {
        Map<Integer, List<Entry>> featured = includeFeatured ? loadFeaturedArtists() : Map.of();
        Map<Integer, ArtistTotals> totals = new LinkedHashMap<>();
        loadCharts("song").forEach((key, entries) -> {
            boolean seasonal = "seasonal".equals(key.periodType());
            for (Entry entry : entries) {
                if (entry.position() == null || entry.artistId() == null) continue;
                totals.computeIfAbsent(entry.artistId(), id -> new ArtistTotals(entry))
                        .add(entry.position(), seasonal, key.periodKey());
                for (Entry guest : featured.getOrDefault(entry.itemId(), List.of())) {
                    if (guest.artistId().equals(entry.artistId())) continue;
                    totals.computeIfAbsent(guest.artistId(), id -> new ArtistTotals(guest))
                            .add(entry.position(), seasonal, key.periodKey());
                }
            }
        });
        return totals.values().stream()
                .map(ArtistTotals::row)
                .sorted(Comparator.comparingInt(ArtistRow::seasonPoints).reversed()
                        .thenComparing(Comparator.comparingInt(ArtistRow::yearPoints).reversed())
                        .thenComparing(ArtistRow::name, String.CASE_INSENSITIVE_ORDER))
                .toList();
    }

    private static final class ArtistTotals {
        private final Entry first;
        private int seasonPoints, seasonEntries, seasonOnes, seasonTop10, bubbling;
        private int yearPoints, yearEntries, yearOnes, yearTop10;
        private String firstKey, lastKey;

        ArtistTotals(Entry first) {
            this.first = first;
        }

        void add(int position, boolean seasonal, String periodKey) {
            if (seasonal && position > CHARTED_LIMIT) {
                bubbling++;
                return;
            }
            int points = Math.max(0, CHARTED_LIMIT + 1 - position);
            if (seasonal) {
                seasonPoints += points;
                seasonEntries++;
                if (position == 1) seasonOnes++;
                if (position <= 10) seasonTop10++;
                if (firstKey == null || seasonOrder(periodKey) < seasonOrder(firstKey)) firstKey = periodKey;
                if (lastKey == null || seasonOrder(periodKey) > seasonOrder(lastKey)) lastKey = periodKey;
            } else {
                yearPoints += points;
                yearEntries++;
                if (position == 1) yearOnes++;
                if (position <= 10) yearTop10++;
            }
        }

        ArtistRow row() {
            return new ArtistRow(first.artistId(), first.artistName(), gender(first.artistGender()), first.artistCoverUrl(),
                    seasonPoints, seasonEntries, seasonOnes, seasonTop10, bubbling,
                    yearPoints, yearEntries, yearOnes, yearTop10,
                    firstKey == null ? null : seasonLabel(firstKey), firstKey == null ? Integer.MAX_VALUE : seasonOrder(firstKey),
                    lastKey == null ? null : seasonLabel(lastKey), lastKey == null ? Integer.MIN_VALUE : seasonOrder(lastKey));
        }
    }

    private Summary summary(Map<ChartKey, List<Entry>> charts, Map<ChartKey, Tile> tiles, String tab) {
        int charted = 0;
        int waiting = 0;
        List<String> seasonKeys = new ArrayList<>();
        for (Map.Entry<ChartKey, Tile> item : tiles.entrySet()) {
            if (!"seasonal".equals(item.getKey().periodType())) continue;
            if ("chart".equals(item.getValue().state())) {
                charted++;
                seasonKeys.add(item.getKey().periodKey());
            } else if (item.getValue().draft()) {
                waiting++;
            }
        }
        seasonKeys.sort(Comparator.comparingInt(SeasonalWallService::seasonOrder));
        String first = seasonKeys.isEmpty() ? null : seasonLabel(seasonKeys.get(0));
        String last = seasonKeys.isEmpty() ? null : seasonLabel(seasonKeys.get(seasonKeys.size() - 1));

        Map<String, String> seasonLeaders = leaders(tiles, "seasonal");
        Map<String, String> yearLeaders = leaders(tiles, "yearly");

        // Everything that reached the Yearly chart, and its best position there.
        String base = "album".equals(tab) ? "/albums/" : "/songs/";
        Map<String, Integer> yearlyBest = new HashMap<>();
        charts.forEach((key, entries) -> {
            if (!"yearly".equals(key.periodType())) return;
            for (Entry entry : charted(listed(entries), key.periodType())) {
                yearlyBest.merge(base + entry.itemId(), entry.position(), Math::min);
            }
        });
        int onYearly = 0;
        int inTop10 = 0;
        for (String id : seasonLeaders.keySet()) {
            Integer best = yearlyBest.get(id);
            if (best != null) {
                onYearly++;
                if (best <= 10) inTop10++;
            }
        }
        return new Summary(charted, waiting, first, last, counts(seasonLeaders), counts(yearLeaders), onYearly, inTop10);
    }

    /** Distinct tile leaders of one period type, keyed by their link, with their gender. */
    private static Map<String, String> leaders(Map<ChartKey, Tile> tiles, String periodType) {
        Map<String, String> out = new LinkedHashMap<>();
        tiles.forEach((key, tile) -> {
            if (periodType.equals(key.periodType()) && "chart".equals(tile.state()) && tile.link() != null) {
                out.putIfAbsent(tile.link(), tile.gender());
            }
        });
        return out;
    }

    private static GenderCounts counts(Map<String, String> genders) {
        int male = 0;
        int female = 0;
        int other = 0;
        for (String gender : genders.values()) {
            if ("male".equals(gender)) male++;
            else if ("female".equals(gender)) female++;
            else other++;
        }
        return new GenderCounts(male, female, other);
    }

    private Tile tile(ChartKey key, List<Entry> entries, String tab) {
        String label = "yearly".equals(key.periodType()) ? key.periodKey() : seasonLabel(key.periodKey());
        boolean draft = entries.stream().noneMatch(Entry::finalized);
        List<Entry> listed = listed(entries);
        List<Entry> charted = charted(listed, key.periodType());
        if (charted.isEmpty()) {
            return new Tile("empty", key.periodType(), key.periodKey(), label, null, null, null, null, null,
                    null, List.of(), 0, 0, 0, draft, false);
        }
        // One dot per listed position; seasonal bubbling-under (31-35) dots are marked "bub".
        List<String> dots = new ArrayList<>();
        for (Entry entry : listed) {
            boolean bubbling = "seasonal".equals(key.periodType()) && entry.position() > CHARTED_LIMIT;
            dots.add(gender(entry.gender()) + (bubbling ? " bub" : ""));
        }
        int male = 0;
        int female = 0;
        for (Entry entry : charted) {
            String gender = gender(entry.gender());
            if ("male".equals(gender)) male++;
            else if ("female".equals(gender)) female++;
        }
        Entry top = charted.get(0);
        String base = "album".equals(tab) ? "/albums/" : "/songs/";
        return new Tile("chart", key.periodType(), key.periodKey(), label, top.itemName(), top.artistName(),
                base + top.itemId(), top.coverUrl(), gender(top.gender()),
                top.position(), dots, male, female, charted.size(), draft, false);
    }

    /** Entries with an item and a position, best position first. */
    private static List<Entry> listed(List<Entry> entries) {
        List<Entry> out = new ArrayList<>();
        for (Entry entry : entries) {
            if (entry.position() != null && entry.itemId() != null) out.add(entry);
        }
        out.sort(Comparator.comparingInt(Entry::position));
        return out;
    }

    /** Listed entries that count as charted: seasonal positions above 30 are bubbling under. */
    private static List<Entry> charted(List<Entry> listed, String periodType) {
        if (!"seasonal".equals(periodType)) return listed;
        return listed.stream().filter(entry -> entry.position() <= CHARTED_LIMIT).toList();
    }

    private static Tile missing(String periodType, String label, boolean later) {
        return new Tile(later ? "later" : "none", periodType, null, label, null, null, null, null, null,
                null, List.of(), 0, 0, 0, false, false);
    }

    static String gender(String name) {
        if (name == null) return "other";
        String lower = name.trim().toLowerCase();
        return "male".equals(lower) || "female".equals(lower) ? lower : "other";
    }

    static Integer yearOf(String periodKey) {
        if (periodKey == null || periodKey.length() < 4) return null;
        try {
            return Integer.valueOf(periodKey.substring(0, 4));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static int seasonIndex(String periodKey) {
        int dash = periodKey == null ? -1 : periodKey.indexOf('-');
        return dash < 0 ? -1 : SEASONS.indexOf(periodKey.substring(dash + 1));
    }

    static int seasonOrder(String periodKey) {
        Integer year = yearOf(periodKey);
        return year == null ? Integer.MIN_VALUE : year * 4 + Math.max(0, seasonIndex(periodKey));
    }

    static String seasonLabel(String periodKey) {
        int index = seasonIndex(periodKey);
        Integer year = yearOf(periodKey);
        return index < 0 || year == null ? periodKey : SEASONS.get(index) + " " + year;
    }

    /** Featured artists of every song on a seasonal or yearly chart, keyed by song id (artist fields only). */
    private Map<Integer, List<Entry>> loadFeaturedArtists() {
        Map<Integer, List<Entry>> out = new HashMap<>();
        jdbcTemplate.query("""
                SELECT DISTINCT sfa.song_id, ar.id AS artist_id, ar.name AS artist_name, g.name AS artist_gender,
                       CASE WHEN ar.image IS NOT NULL OR EXISTS (SELECT 1 FROM ArtistImage xi WHERE xi.artist_id = ar.id) THEN 1 ELSE 0 END AS artist_img
                FROM SongFeaturedArtist sfa
                INNER JOIN Artist ar ON ar.id = sfa.artist_id
                LEFT JOIN Gender g ON g.id = ar.gender_id
                WHERE sfa.song_id IN (
                    SELECT ce.song_id FROM ChartEntry ce INNER JOIN Chart c ON c.id = ce.chart_id
                    WHERE c.chart_type = 'song' AND c.period_type IN ('seasonal', 'yearly'))
                """, rs -> {
            int artistId = rs.getInt("artist_id");
            String cover = rs.getInt("artist_img") == 1 ? "/artists/" + artistId + "/image?thumbnail=true" : null;
            out.computeIfAbsent(rs.getInt("song_id"), id -> new ArrayList<>())
                    .add(new Entry(null, null, false, null, null, null, artistId, rs.getString("artist_name"),
                            null, rs.getString("artist_gender"), null, cover));
        });
        return out;
    }

    private Map<ChartKey, List<Entry>> loadCharts(String chartType) {
        String sql = "album".equals(chartType) ? """
                SELECT c.period_type, c.period_key, c.is_finalized, ce.position,
                       a.id AS item_id, a.name AS item_name, ar.id AS artist_id, ar.name AS artist_name,
                       g.name AS gender, g.name AS artist_gender,
                       CASE WHEN a.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = a.id) THEN 1 ELSE 0 END AS item_img,
                       NULL AS album_id, 0 AS album_img,
                       CASE WHEN ar.image IS NOT NULL OR EXISTS (SELECT 1 FROM ArtistImage xi WHERE xi.artist_id = ar.id) THEN 1 ELSE 0 END AS artist_img
                FROM Chart c
                LEFT JOIN ChartEntry ce ON ce.chart_id = c.id
                LEFT JOIN Album a ON a.id = ce.album_id
                LEFT JOIN Artist ar ON ar.id = a.artist_id
                LEFT JOIN Gender g ON g.id = ar.gender_id
                WHERE c.chart_type = 'album' AND c.period_type IN ('seasonal', 'yearly')
                ORDER BY c.period_type, c.period_key, ce.position
                """ : """
                SELECT c.period_type, c.period_key, c.is_finalized, ce.position,
                       s.id AS item_id, s.name AS item_name, ar.id AS artist_id, ar.name AS artist_name,
                       g.name AS gender, ag.name AS artist_gender,
                       CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage si WHERE si.song_id = s.id) THEN 1 ELSE 0 END AS item_img,
                       s.album_id AS album_id,
                       CASE WHEN al.image IS NOT NULL OR EXISTS (SELECT 1 FROM AlbumImage ai WHERE ai.album_id = al.id) THEN 1 ELSE 0 END AS album_img,
                       CASE WHEN ar.image IS NOT NULL OR EXISTS (SELECT 1 FROM ArtistImage xi WHERE xi.artist_id = ar.id) THEN 1 ELSE 0 END AS artist_img
                FROM Chart c
                LEFT JOIN ChartEntry ce ON ce.chart_id = c.id
                LEFT JOIN Song s ON s.id = ce.song_id
                LEFT JOIN Album al ON al.id = s.album_id
                LEFT JOIN Artist ar ON ar.id = s.artist_id
                LEFT JOIN Gender g ON g.id = COALESCE(s.override_gender_id, ar.gender_id)
                LEFT JOIN Gender ag ON ag.id = ar.gender_id
                WHERE c.chart_type = 'song' AND c.period_type IN ('seasonal', 'yearly')
                ORDER BY c.period_type, c.period_key, ce.position
                """;
        Map<ChartKey, List<Entry>> charts = new LinkedHashMap<>();
        String itemBase = "album".equals(chartType) ? "/albums/" : "/songs/";
        jdbcTemplate.query(sql, rs -> {
            ChartKey key = new ChartKey(rs.getString("period_type"), rs.getString("period_key"));
            List<Entry> entries = charts.computeIfAbsent(key, k -> new ArrayList<>());
            int position = rs.getInt("position");
            Integer pos = rs.wasNull() ? null : position;
            int itemIdRaw = rs.getInt("item_id");
            Integer itemId = rs.wasNull() ? null : itemIdRaw;
            int artistIdRaw = rs.getInt("artist_id");
            Integer artistId = rs.wasNull() ? null : artistIdRaw;
            int albumIdRaw = rs.getInt("album_id");
            Integer albumId = rs.wasNull() ? null : albumIdRaw;
            // Same rule as the chart overview: album art first, the song's own art otherwise.
            String cover = albumId != null && rs.getInt("album_img") == 1 ? "/albums/" + albumId + "/image?thumbnail=true"
                    : itemId != null && rs.getInt("item_img") == 1 ? itemBase + itemId + "/image?thumbnail=true" : null;
            String artistCover = artistId != null && rs.getInt("artist_img") == 1 ? "/artists/" + artistId + "/image?thumbnail=true" : null;
            entries.add(new Entry(key.periodType(), key.periodKey(), rs.getInt("is_finalized") == 1, pos,
                    itemId, rs.getString("item_name"), artistId, rs.getString("artist_name"),
                    rs.getString("gender"), rs.getString("artist_gender"), cover, artistCover));
        });
        return charts;
    }
}
