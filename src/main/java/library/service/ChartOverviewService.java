package library.service;

import library.dto.BillboardHot100OverviewRowDTO;
import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartOverviewRows.AlbumRow;
import library.dto.ChartOverviewRows.ArtistRow;
import library.dto.ChartOverviewRows.SongRow;
import library.dto.ChartSongOverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.entity.TrlDebut;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Backs the shared Chart Overview page: one row model, one multi-level sort and one summary
 * for the Weekly, Hot 100, TRL and Vato's Cuntdown charts (songs, albums and artists).
 * Rows come from the existing chart services (cached through {@link OverviewCacheService})
 * and are filtered with {@link OverviewFilterService} before they are mapped.
 */
@Service
public class ChartOverviewService {

    /** The charts the page covers. {@code source} is the key the filter service and caches use. */
    public enum Chart {
        WEEKLY("weekly", "Weekly", "weekly", "week", new int[]{1, 5, 10}),
        HOT100("hot100", "Hot 100", "billboard", "week", new int[]{1, 5, 10, 20, 50, 100}),
        TRL("trl", "TRL", "trl", "day", new int[]{1, 5, 10}),
        CUNTDOWN("cuntdown", "Vato's Cuntdown", "pc", "day", new int[]{1, 5, 10});

        private final String key;
        private final String label;
        private final String source;
        private final String unit;
        private final int[] tiers;

        Chart(String key, String label, String source, String unit, int[] tiers) {
            this.key = key;
            this.label = label;
            this.source = source;
            this.unit = unit;
            this.tiers = tiers;
        }

        public String key() { return key; }
        public String label() { return label; }
        public String source() { return source; }
        public String unit() { return unit; }
        public int[] tiers() { return tiers.clone(); }

        public static Chart from(String value) {
            if (value != null) {
                String normalized = value.trim().toLowerCase(Locale.ROOT);
                for (Chart chart : values()) {
                    if (chart.key.equals(normalized) || chart.source.equals(normalized)) {
                        return chart;
                    }
                }
            }
            return WEEKLY;
        }
    }

    public record SortSpec(String key, boolean ascending) {
    }

    public record Page(List<?> rows, int totalCount, boolean hasMore, int page, Map<String, Object> summary) {
    }

    /** One week (or day) of a chart run; {@code position} is null when the song was off the chart. */
    public record RunPoint(String key, String label, Integer position) {
    }

    private static final String NAME_KEY_PREFIX = "n:";
    private static final String NAME_KEY_SEPARATOR = "||";

    private static final int LEADERBOARD_SIZE = 25;

    private final ChartService chartService;
    private final TrlService trlService;
    private final PcService pcService;
    private final BillboardHot100Service billboardService;
    private final OverviewFilterService filterService;
    private final OverviewCacheService cacheService;

    public ChartOverviewService(ChartService chartService, TrlService trlService, PcService pcService,
                                BillboardHot100Service billboardService, OverviewFilterService filterService,
                                OverviewCacheService cacheService) {
        this.chartService = chartService;
        this.trlService = trlService;
        this.pcService = pcService;
        this.billboardService = billboardService;
        this.filterService = filterService;
        this.cacheService = cacheService;
    }

    public static String normalizeTab(String tab) {
        return "album".equals(tab) || "artist".equals(tab) ? tab : "song";
    }

    /** Parses repeated {@code sort=key:dir} values, drops unknown and repeated keys, and falls back to the tab default. */
    public static List<SortSpec> parseSorts(String tab, List<String> raw) {
        Map<String, ?> keys = sortKeys(normalizeTab(tab));
        List<SortSpec> sorts = new ArrayList<>();
        if (raw != null) {
            for (String value : raw) {
                if (value == null || value.isBlank()) continue;
                String[] parts = value.split(":", 2);
                String key = parts[0].trim();
                if (!keys.containsKey(key) || sorts.stream().anyMatch(spec -> spec.key().equals(key))) continue;
                boolean ascending = parts.length > 1 && "asc".equalsIgnoreCase(parts[1].trim());
                sorts.add(new SortSpec(key, ascending));
            }
        }
        if (sorts.isEmpty()) {
            sorts.add("song".equals(normalizeTab(tab)) ? new SortSpec("debutDate", false) : new SortSpec("span", false));
        }
        return sorts;
    }

    public Page page(Chart chart, String tab, List<SortSpec> sorts, boolean includeFeatured,
                     Integer topSong, Integer topAlbum, MultiValueMap<String, String> filterParams, int page, int size) {
        String safeTab = normalizeTab(tab);
        List<?> rows = switch (safeTab) {
            case "album" -> sorted(albumRows(chart, filterParams), albumSortKeys(), sorts);
            case "artist" -> sorted(artistRows(chart, includeFeatured, topSong, topAlbum, filterParams), artistSortKeys(), sorts);
            default -> sorted(songRows(chart, filterParams), songSortKeys(), sorts);
        };
        int safePage = Math.max(0, page);
        int from = (int) Math.min((long) safePage * size, rows.size());
        int to = Math.min(rows.size(), from + size);
        Map<String, Object> summary = safePage == 0 ? summarize(safeTab, rows) : null;
        return new Page(new ArrayList<>(rows.subList(from, to)), rows.size(), to < rows.size(), safePage, summary);
    }

    /** Forgets the cached rows of an external chart so the next page shows edits made a moment ago. */
    public void invalidate(Chart chart) {
        if (chart != Chart.WEEKLY) {
            cacheService.invalidate(chart.source() + ":");
        }
    }

    // ---------------------------------------------------------------- rows

    public List<SongRow> songRows(Chart chart, MultiValueMap<String, String> filterParams) {
        return switch (chart) {
            case WEEKLY -> filterService.filter("weekly", "song", chartService.getChartOverviewSongRows("weekly"), filterParams)
                    .stream().map(ChartOverviewService::weeklySong).toList();
            case HOT100 -> filterService.filter("billboard", "song", billboardSongs(), filterParams)
                    .stream().map(ChartOverviewService::billboardSong).toList();
            case TRL -> filterService.filter("trl", "song", trlSongs(), filterParams)
                    .stream().map(ChartOverviewService::trlSong).toList();
            case CUNTDOWN -> filterService.filter("pc", "song", pcSongs(), filterParams)
                    .stream().map(ChartOverviewService::pcSong).toList();
        };
    }

    List<AlbumRow> albumRows(Chart chart, MultiValueMap<String, String> filterParams) {
        List<ChartAlbumOverviewRowDTO> rows = switch (chart) {
            case WEEKLY -> chartService.getChartOverviewAlbumRows("weekly");
            case HOT100 -> cacheService.get("billboard:album", () -> billboardService.getAlbumOverviewRows(billboardSongs()));
            case TRL -> cacheService.get("trl:album", () -> trlService.getAlbumOverviewRows(trlSongs()));
            case CUNTDOWN -> cacheService.get("pc:album", () -> pcService.getAlbumOverviewRows(pcSongs()));
        };
        return filterService.filter(chart.source(), "album", rows, filterParams).stream().map(ChartOverviewService::album).toList();
    }

    List<ArtistRow> artistRows(Chart chart, boolean includeFeatured, Integer topSong, Integer topAlbum,
                               MultiValueMap<String, String> filterParams) {
        List<ChartArtistOverviewRowDTO> rows = switch (chart) {
            case WEEKLY -> chartService.getChartOverviewArtistRows("weekly", chartService.getChartOverviewSongRows("weekly"),
                    chartService.getChartOverviewAlbumRows("weekly"), includeFeatured);
            case HOT100 -> cacheService.get("billboard:artist:" + includeFeatured,
                    () -> billboardService.getArtistOverviewRows(billboardSongs(), includeFeatured));
            case TRL -> cacheService.get("trl:artist:" + includeFeatured,
                    () -> trlService.getArtistOverviewRows(trlSongs(), includeFeatured));
            case CUNTDOWN -> cacheService.get("pc:artist:" + includeFeatured,
                    () -> pcService.getArtistOverviewRows(pcSongs(), includeFeatured));
        };
        return filterService.filter(chart.source(), "artist", rows, filterParams).stream()
                .map(row -> artist(row, topSong, topAlbum)).toList();
    }

    private List<BillboardHot100OverviewRowDTO> billboardSongs() {
        return cacheService.get("billboard:song", billboardService::getAllOverviewRows);
    }

    private List<TrlDebut> trlSongs() {
        return cacheService.get("trl:song", trlService::getAllDebuts);
    }

    private List<PcOverviewRowDTO> pcSongs() {
        return cacheService.get("pc:song", pcService::getOverviewRows);
    }

    static SongRow weeklySong(ChartSongOverviewRowDTO r) {
        return new SongRow(r.getSongId() == null ? null : String.valueOf(r.getSongId()), r.getSongId(), r.getAlbumId(), r.getArtistId(),
                r.getSongTitle(), r.getArtistName(), r.getAlbumName(), gender(r.getGenderClass()), true,
                cover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                hover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                r.getTotalChartSpan(), null, r.getPeakPosition(), r.getSpanAtPeak(), r.getDebutPosition(),
                r.getFirstAppearanceSortValue(), r.getFirstAppearanceKey(),
                r.getPeakAppearanceSortValue(), r.getPeakAppearanceKey(),
                r.getLastAppearanceSortValue(), r.getLastAppearanceKey(),
                new int[]{r.getSpanAtTop1(), r.getSpanAtTop5(), r.getSpanAtTop10()},
                false, 0, r.getLinkedSongTitles());
    }

    static SongRow billboardSong(BillboardHot100OverviewRowDTO r) {
        return new SongRow(runKey(r.getSongId(), r.getArtistName(), r.getSongTitle()), r.getSongId(), r.getAlbumId(), r.getResolvedArtistId(),
                r.getSongTitle(), r.getArtistName(), null, gender(r.getGenderClass()), r.isMatched(),
                cover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                hover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                r.getWeeksOnChart(), null, r.getPeakPosition(), r.getWeeksAtPeak(), r.getDebutPosition(),
                r.getFirstWeek(), r.getFirstWeek(), r.getPeakWeek(), r.getPeakWeek(), r.getLastWeek(), r.getLastWeek(),
                new int[]{r.getWeeksAtTop1(), r.getWeeksAtTop5(), r.getWeeksAtTop10(), r.getWeeksAtTop20(), r.getWeeksAtTop50(), r.getWeeksAtTop100()},
                false, 0, null);
    }

    static SongRow trlSong(TrlDebut r) {
        return new SongRow(r.getId() == null ? null : String.valueOf(r.getId()), r.getSongId(), r.getAlbumId(), r.getResolvedArtistId(),
                r.getSongTitle(), r.getArtistName(), null, gender(r.getGenderClass()), r.getSongId() != null,
                cover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                hover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                orZero(r.getDaysOnCountdown()), r.getActualDays(), r.getPeakPosition(), orZero(r.getDaysAtPeak()), r.getDebutPosition(),
                r.getDebutDate(), r.getDebutDate(), r.getPeakDate(), r.getPeakDate(), r.getLastAppearanceDate(), r.getLastAppearanceDate(),
                new int[]{r.getDaysAtTop1(), r.getDaysAtTop5(), r.getDaysAtTop10()},
                r.isRetired(), 0, null);
    }

    static SongRow pcSong(PcOverviewRowDTO r) {
        return new SongRow(runKey(r.getSongId(), r.getArtistName(), r.getSongTitle()), r.getSongId(), r.getAlbumId(), r.getResolvedArtistId(),
                r.getSongTitle(), r.getArtistName(), null, gender(r.getGenderClass()), r.isMatched(),
                cover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                hover(r.getSongId(), r.isHasImage(), r.getAlbumId(), r.isAlbumHasImage()),
                r.getDaysOnCountdown(), null, r.getPeakPosition(), r.getDaysAtPeak(), r.getDebutPosition(),
                r.getFirstWeek(), r.getFirstWeek(), r.getPeakWeek(), r.getPeakWeek(), r.getLastWeek(), r.getLastWeek(),
                new int[]{r.getDaysAtTop1(), r.getDaysAtTop5(), r.getDaysAtTop10()},
                false, r.getRawVariantCount(), null);
    }

    static AlbumRow album(ChartAlbumOverviewRowDTO r) {
        return new AlbumRow(r.getAlbumId(), r.getResolvedArtistId(), r.getAlbumName(), r.getArtistName(), gender(r.getGenderClass()),
                r.getAlbumId() != null && r.isHasImage() ? "/albums/" + r.getAlbumId() + "/image?thumbnail=true" : null,
                r.getChartedSongsCount(), r.getTotalChartSpan(), r.getHighestPeak(), r.getSpanAtPeak(),
                r.getNumberOneSongsCount(), r.getTotalSpanAtNumberOne(), r.getDebutPosition(),
                isoOr(r.getFirstDebutSortValue(), r.getFirstDebutDate()), keyOr(r.getFirstDebutKey(), r.getFirstDebutDate()),
                isoOr(r.getPeakAppearanceSortValue(), r.getPeakAppearanceDate()), keyOr(r.getPeakAppearanceKey(), r.getPeakAppearanceDate()),
                isoOr(r.getLastAppearanceSortValue(), r.getLastAppearanceDate()), keyOr(r.getLastAppearanceKey(), r.getLastAppearanceDate()),
                r.getNumberOneSongTitles());
    }

    static ArtistRow artist(ChartArtistOverviewRowDTO r, Integer topSong, Integer topAlbum) {
        return new ArtistRow(r.getResolvedArtistId(), r.getArtistName(), gender(r.getGenderClass()), r.isMatched(),
                r.getResolvedArtistId() != null && r.isHasImage() ? "/artists/" + r.getResolvedArtistId() + "/image?thumbnail=true" : null,
                threshold(r.getTopSongCounts(), topSong, r.getChartedSongsCount()),
                threshold(r.getTopSongWeeks(), topSong, r.getTotalChartSpan()),
                r.getHighestPeak(), r.getNumberOneSongsCount(), r.getTotalSpanAtNumberOne(),
                threshold(r.getTopAlbumCounts(), topAlbum, r.getChartedAlbumsCount()),
                threshold(r.getTopAlbumWeeks(), topAlbum, r.getAlbumTotalChartSpan()),
                r.getAlbumHighestPeak(), r.getNumberOneAlbumsCount(), r.getAlbumTotalSpanAtNumberOne(),
                isoOr(r.getFirstDebutSortValue(), r.getFirstDebutDate()), keyOr(r.getFirstDebutKey(), r.getFirstDebutDate()),
                isoOr(r.getLastAppearanceSortValue(), r.getLastAppearanceDate()), keyOr(r.getLastAppearanceKey(), r.getLastAppearanceDate()),
                r.getNumberOneSongTitles(), r.getNumberOneAlbumTitles());
    }

    /** Hot 100 and Cuntdown runs are looked up by song, or by the raw chart names when the entry is not linked yet. */
    private static String runKey(Integer songId, String artist, String song) {
        if (songId != null) return String.valueOf(songId);
        if (artist == null || song == null) return null;
        return NAME_KEY_PREFIX + artist + NAME_KEY_SEPARATOR + song;
    }

    // ---------------------------------------------------------------- chart runs

    /** Chart runs for the given row keys (the rows' {@code runKey}); unknown keys are skipped. */
    public Map<String, List<RunPoint>> runs(Chart chart, List<String> keys) {
        Map<String, List<RunPoint>> runs = new LinkedHashMap<>();
        if (keys == null) return runs;
        for (String key : keys) {
            if (key == null || key.isBlank() || runs.containsKey(key)) continue;
            try {
                List<RunPoint> run = run(chart, key);
                if (run != null) runs.put(key, run);
            } catch (RuntimeException ignored) {
                // A broken run must not hide the others; the row shows "No chart entries found."
            }
        }
        return runs;
    }

    private List<RunPoint> run(Chart chart, String key) {
        if (chart == Chart.WEEKLY) {
            Integer songId = parseId(key);
            if (songId == null) return null;
            var run = chartService.getSongChartRunAllTime(songId);
            if (run == null || run.getWeeks() == null) return List.of();
            return run.getWeeks().stream()
                    .map(week -> new RunPoint(week.getPeriodKey(), week.getDateRange(), week.isOnChart() ? week.getPosition() : null))
                    .toList();
        }
        List<Map<String, Object>> raw;
        if (key.startsWith(NAME_KEY_PREFIX)) {
            String names = key.substring(NAME_KEY_PREFIX.length());
            int split = names.indexOf(NAME_KEY_SEPARATOR);
            if (split < 0) return null;
            String artist = names.substring(0, split);
            String song = names.substring(split + NAME_KEY_SEPARATOR.length());
            raw = switch (chart) {
                case HOT100 -> billboardService.getChartRunByNames(artist, song);
                case CUNTDOWN -> pcService.getChartRunByNames(artist, song);
                case TRL -> trlService.getChartRunByNames(artist, song);
                default -> List.of();
            };
        } else {
            Integer id = parseId(key);
            if (id == null) return null;
            raw = switch (chart) {
                case HOT100 -> billboardService.getChartRunBySongId(id);
                case CUNTDOWN -> pcService.getChartRunBySongId(id);
                case TRL -> trlService.getChartRunForDebut(id);
                default -> List.of();
            };
        }
        List<RunPoint> points = new ArrayList<>();
        for (Map<String, Object> entry : raw == null ? List.<Map<String, Object>>of() : raw) {
            String date = entry.get("chart_date") == null ? null : String.valueOf(entry.get("chart_date"));
            boolean onChart = entry.get("on_chart") instanceof Number number ? number.intValue() != 0 : Boolean.TRUE.equals(entry.get("on_chart"));
            Integer position = onChart && entry.get("position") instanceof Number number ? number.intValue() : null;
            points.add(new RunPoint(date, date, position));
        }
        return points;
    }

    private static Integer parseId(String key) {
        try {
            return Integer.valueOf(key.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int threshold(int[] values, Integer threshold, int fallback) {
        if (values == null || threshold == null || threshold < 1 || threshold >= values.length) {
            return fallback;
        }
        return values[threshold];
    }

    static String gender(String genderClass) {
        if ("gender-male".equals(genderClass)) return "male";
        if ("gender-female".equals(genderClass)) return "female";
        return null;
    }

    private static String cover(Integer songId, boolean songImage, Integer albumId, boolean albumImage) {
        if (albumId != null && albumImage) return "/albums/" + albumId + "/image?thumbnail=true";
        if (songId != null && songImage) return "/songs/" + songId + "/image?thumbnail=true";
        return null;
    }

    /** The song's own art, shown on hover when the album art is the default cover. */
    private static String hover(Integer songId, boolean songImage, Integer albumId, boolean albumImage) {
        return songId != null && songImage && albumId != null && albumImage ? "/songs/" + songId + "/image?thumbnail=true" : null;
    }

    private static String isoOr(String sortValue, String fallback) {
        String value = sortValue != null && !sortValue.isBlank() ? sortValue : fallback;
        return value != null && value.matches("\\d{4}-\\d{2}-\\d{2}.*") ? value.substring(0, 10) : value;
    }

    private static String keyOr(String key, String fallback) {
        return key != null && !key.isBlank() ? key : fallback;
    }

    private static int orZero(Integer value) {
        return value == null ? 0 : value;
    }

    // ---------------------------------------------------------------- sorting

    private static Map<String, ?> sortKeys(String tab) {
        return switch (tab) {
            case "album" -> albumSortKeys();
            case "artist" -> artistSortKeys();
            default -> songSortKeys();
        };
    }

    static Map<String, Function<SongRow, Comparable<?>>> songSortKeys() {
        Map<String, Function<SongRow, Comparable<?>>> keys = new LinkedHashMap<>();
        keys.put("title", row -> lower(row.title()));
        keys.put("artist", row -> lower(row.artistName()));
        keys.put("span", SongRow::span);
        keys.put("actualSpan", SongRow::actualSpan);
        keys.put("peak", SongRow::peak);
        keys.put("spanAtPeak", SongRow::spanAtPeak);
        keys.put("debutDate", SongRow::debutDate);
        keys.put("debutPosition", SongRow::debutPosition);
        keys.put("peakDate", SongRow::peakDate);
        keys.put("lastDate", SongRow::lastDate);
        String[] tierKeys = {"top1", "top5", "top10", "top20", "top50", "top100"};
        for (int i = 0; i < tierKeys.length; i++) {
            int index = i;
            keys.put(tierKeys[i], row -> row.tiers() != null && index < row.tiers().length ? row.tiers()[index] : null);
        }
        return keys;
    }

    static Map<String, Function<AlbumRow, Comparable<?>>> albumSortKeys() {
        Map<String, Function<AlbumRow, Comparable<?>>> keys = new LinkedHashMap<>();
        keys.put("title", row -> lower(row.albumName()));
        keys.put("artist", row -> lower(row.artistName()));
        keys.put("songs", AlbumRow::songs);
        keys.put("span", AlbumRow::span);
        keys.put("peak", AlbumRow::peak);
        keys.put("spanAtPeak", AlbumRow::spanAtPeak);
        keys.put("numberOnes", AlbumRow::numberOnes);
        keys.put("spanAtNumberOne", AlbumRow::spanAtNumberOne);
        keys.put("debutDate", AlbumRow::debutDate);
        keys.put("debutPosition", AlbumRow::debutPosition);
        keys.put("peakDate", AlbumRow::peakDate);
        keys.put("lastDate", AlbumRow::lastDate);
        return keys;
    }

    static Map<String, Function<ArtistRow, Comparable<?>>> artistSortKeys() {
        Map<String, Function<ArtistRow, Comparable<?>>> keys = new LinkedHashMap<>();
        keys.put("artist", row -> lower(row.artistName()));
        keys.put("songs", ArtistRow::songs);
        keys.put("span", ArtistRow::span);
        keys.put("peak", ArtistRow::peak);
        keys.put("numberOnes", ArtistRow::numberOnes);
        keys.put("spanAtNumberOne", ArtistRow::spanAtNumberOne);
        keys.put("albums", ArtistRow::albums);
        keys.put("albumSpan", ArtistRow::albumSpan);
        keys.put("albumPeak", ArtistRow::albumPeak);
        keys.put("albumNumberOnes", ArtistRow::albumNumberOnes);
        keys.put("albumSpanAtNumberOne", ArtistRow::albumSpanAtNumberOne);
        keys.put("debutDate", ArtistRow::debutDate);
        keys.put("lastDate", ArtistRow::lastDate);
        return keys;
    }

    private static String lower(String value) {
        return value == null ? null : value.toLowerCase(Locale.ROOT);
    }

    /** Sorts by every level in order; blanks always go last, whatever the direction. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> List<T> sorted(List<T> rows, Map<String, Function<T, Comparable<?>>> keys, List<SortSpec> sorts) {
        Comparator<T> comparator = (a, b) -> 0;
        for (SortSpec spec : sorts) {
            Function<T, Comparable<?>> extractor = keys.get(spec.key());
            if (extractor == null) continue;
            Comparator<Comparable> natural = spec.ascending() ? Comparator.naturalOrder() : Comparator.reverseOrder();
            comparator = comparator.thenComparing(row -> (Comparable) extractor.apply(row), Comparator.nullsLast(natural));
        }
        Function<T, Comparable<?>> artist = keys.get("artist");
        Function<T, Comparable<?>> title = keys.get("title");
        if (artist != null) comparator = comparator.thenComparing(row -> (Comparable) artist.apply(row), Comparator.nullsLast(Comparator.naturalOrder()));
        if (title != null) comparator = comparator.thenComparing(row -> (Comparable) title.apply(row), Comparator.nullsLast(Comparator.naturalOrder()));
        List<T> copy = new ArrayList<>(rows);
        copy.sort(comparator);
        return copy;
    }

    // ---------------------------------------------------------------- summary

    /** Header numbers for the filtered rows: gender counts, peak spread per gender and, for artists, the leaderboards. */
    Map<String, Object> summarize(String tab, List<?> rows) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total", rows.size());
        switch (tab) {
            case "album" -> {
                List<AlbumRow> albums = cast(rows);
                summary.put("genders", genderCounts(albums, AlbumRow::gender));
                summary.put("peakSpread", peakSpread(albums, AlbumRow::peak, AlbumRow::gender));
            }
            case "artist" -> {
                List<ArtistRow> artists = cast(rows);
                summary.put("genders", genderCounts(artists, ArtistRow::gender));
                summary.put("peakSpread", peakSpread(artists, ArtistRow::peak, ArtistRow::gender));
                summary.put("spanByGender", genderSums(artists, ArtistRow::gender, ArtistRow::span));
                summary.put("numberOneSpanByGender", genderSums(artists, ArtistRow::gender, ArtistRow::spanAtNumberOne));
                summary.put("spanTotal", artists.stream().mapToInt(ArtistRow::span).sum());
                summary.put("albumSpanTotal", artists.stream().mapToInt(ArtistRow::albumSpan).sum());
                summary.put("topMale", topArtists(artists, "male"));
                summary.put("topFemale", topArtists(artists, "female"));
            }
            default -> {
                List<SongRow> songs = cast(rows);
                summary.put("genders", genderCounts(songs, SongRow::gender));
                summary.put("peakSpread", peakSpread(songs, SongRow::peak, SongRow::gender));
            }
        }
        return summary;
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> cast(List<?> rows) {
        return (List<T>) rows;
    }

        private static <T> Map<String, Integer> genderCounts(List<T> rows, Function<T, String> gender) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("male", count(rows, row -> "male".equals(gender.apply(row))));
        counts.put("female", count(rows, row -> "female".equals(gender.apply(row))));
        counts.put("other", rows.size() - counts.get("male") - counts.get("female"));
        return counts;
    }

    private static <T> Map<String, Integer> genderSums(List<T> rows, Function<T, String> gender, Function<T, Integer> value) {
        Map<String, Integer> sums = new LinkedHashMap<>();
        sums.put("male", 0);
        sums.put("female", 0);
        sums.put("other", 0);
        for (T row : rows) {
            String key = gender.apply(row);
            sums.merge(key == null ? "other" : key, value.apply(row), Integer::sum);
        }
        return sums;
    }

    private static <T> int count(List<T> rows, Predicate<T> test) {
        return (int) rows.stream().filter(test).count();
    }

    /** Peak bands #1, #2-5, #6-10, #11+ per gender (male first). */
    private static <T> Map<String, int[]> peakSpread(List<T> rows, Function<T, Integer> peak, Function<T, String> gender) {
        Map<String, int[]> spread = new LinkedHashMap<>();
        spread.put("male", new int[4]);
        spread.put("female", new int[4]);
        spread.put("other", new int[4]);
        for (T row : rows) {
            Integer value = peak.apply(row);
            if (value == null) continue;
            int band = value == 1 ? 0 : value <= 5 ? 1 : value <= 10 ? 2 : 3;
            String key = gender.apply(row);
            spread.get(key == null ? "other" : key)[band]++;
        }
        return spread;
    }

        private static List<ArtistRow> topArtists(List<ArtistRow> rows, String gender) {
        return rows.stream().filter(row -> gender.equals(row.gender()))
                .sorted(Comparator.comparingInt(ArtistRow::span).reversed()
                        .thenComparing(ArtistRow::numberOnes, Comparator.reverseOrder())
                        .thenComparing(row -> lower(row.artistName()), Comparator.nullsLast(Comparator.naturalOrder())))
                .limit(LEADERBOARD_SIZE)
                .toList();
    }
}
