package library.service;

import library.dto.BillboardHot100OverviewRowDTO;
import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;
import library.dto.ChartSongOverviewRowDTO;
import library.dto.PcOverviewRowDTO;
import library.entity.TrlDebut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Applies the list-page filter contract to the fully aggregated overview rows.
 * The chart overview sources already need complete rows for their calculated
 * metrics, so filtering here keeps totals and pagination on the server.
 */
@Service
public class OverviewFilterService {

    private final OverviewFacetService overviewFacetService;

    public OverviewFilterService() {
        this.overviewFacetService = null;
    }

    @Autowired
    public OverviewFilterService(OverviewFacetService overviewFacetService) {
        this.overviewFacetService = overviewFacetService;
    }

    public record FilterField(String name, String label, String type) {
    }

    // Weekly defines the common filter vocabulary. Other overview sources expose
    // the same controls wherever their row type contains the corresponding metric.
    private static final List<FilterField> SONG_FIELDS = List.of(
            field("artist", "Artist", "artist"),
            field("debutPosition", "Debut Position", "number"),
            field("firstAppearance", "First Appearance", "date"),
            field("lastAppearance", "Last Appearance", "date"),
            field("peakAppearance", "Peak Appearance", "date"),
            field("peakPosition", "Peak Position", "number"),
            field("songTitle", "Song", "text"),
            field("spanAtPeak", "Span At Peak", "number"),
            field("totalChartSpan", "Total Chart Span", "number")
    );
    private static final List<FilterField> ALBUM_FIELDS = List.of(
            field("albumName", "Album", "text"),
            field("artist", "Artist", "artist"),
            field("debutPosition", "Debut Position", "number"),
            field("firstAppearance", "First Appearance", "date"),
            field("peakAppearance", "Peak Appearance", "date"),
            field("spanAtPeak", "Span At Peak", "number"),
            field("totalChartSpan", "Total Chart Span", "number"),
            field("highestPeak", "Highest Peak", "number"),
            field("lastAppearance", "Last Appearance", "date")
    );
    private static final List<FilterField> ARTIST_FIELDS = List.of(
            field("albumTotalChartSpan", "Album Total Chart Span", "number"),
            field("artist", "Artist", "artist"),
            field("chartedAlbumsCount", "Charted Albums", "number"),
            field("chartedSongsCount", "Charted Songs", "number"),
            field("numberOneAlbumsCount", "Number One Albums", "number"),
            field("numberOneSongsCount", "Number One Songs", "number"),
            field("totalChartSpan", "Total Chart Span", "number"),
            field("totalSpanAtNumberOne", "Total Span At Number One", "number"),
            field("albumTotalSpanAtNumberOne", "Total Album Span At Number One", "number")
    );
    private static final List<FilterField> EXTERNAL_ALBUM_FIELDS = List.of(
            field("albumName", "Album", "text"),
            field("artist", "Artist", "artist"),
            field("chartedSongsCount", "Songs Charted", "number"),
            field("firstAppearance", "First Debut", "date"),
            field("highestPeak", "Peak", "number"),
            field("lastAppearance", "Last Appearance", "date"),
            field("numberOneSongsCount", "#1 Songs", "number"),
            field("totalChartSpan", "Total Span", "number"),
            field("totalSpanAtNumberOne", "Span at #1", "number")
    );
    private static final List<FilterField> EXTERNAL_ARTIST_FIELDS = List.of(
            field("artist", "Artist", "artist"),
            field("chartedSongsCount", "Songs Charted", "number"),
            field("firstAppearance", "First Debut", "date"),
            field("highestPeak", "Peak", "number"),
            field("lastAppearance", "Last Appearance", "date"),
            field("numberOneSongsCount", "#1 Songs", "number"),
            field("totalChartSpan", "Total Span", "number"),
            field("totalSpanAtNumberOne", "Span at #1", "number")
    );
    private static final List<FilterField> CATALOG_FACET_FIELDS = List.of(
            field("country", "Country", "facet"),
            field("ethnicity", "Ethnicity", "facet"),
            field("gender", "Gender", "facet"),
            field("genre", "Genre", "facet"),
            field("language", "Language", "facet"),
            field("subgenre", "Subgenre", "facet"),
            field("tag", "Tag", "facet")
    );

    public List<FilterField> fieldsFor(String source, String tab) {
        List<FilterField> fields = switch (tab) {
            case "album" -> isExternal(source) ? EXTERNAL_ALBUM_FIELDS : ALBUM_FIELDS;
            case "artist" -> isExternal(source) ? EXTERNAL_ARTIST_FIELDS : ARTIST_FIELDS;
            default -> "trl".equals(source)
                    ? append(SONG_FIELDS, field("actualDays", "Actual Days", "number"), field("retired", "Hall Of Fame", "boolean"))
                    : SONG_FIELDS;
        };
        return append(fields, CATALOG_FACET_FIELDS.toArray(FilterField[]::new)).stream()
                .map(field -> chartLabel(source, tab, field))
                .sorted(Comparator.comparing(FilterField::label, String.CASE_INSENSITIVE_ORDER))
                .toList();
    }

    public <T> List<T> filter(String source, String tab, List<T> rows, MultiValueMap<String, String> params) {
        if (rows == null || rows.isEmpty()) {
            return new ArrayList<>();
        }
        Map<String, FilterField> allowed = fieldsFor(source, tab).stream()
                .collect(java.util.stream.Collectors.toMap(FilterField::name, value -> value));
        String query = first(params, "q");
        List<T> filtered = new ArrayList<>();
        for (T row : rows) {
            if (!matchesQuery(row, query) || !matchesFields(tab, row, allowed, params)) {
                continue;
            }
            filtered.add(row);
        }
        return overviewFacetService == null ? filtered : overviewFacetService.filter(tab, filtered, params);
    }

    private boolean matchesFields(String tab, Object row, Map<String, FilterField> fields, MultiValueMap<String, String> params) {
        for (FilterField field : fields.values()) {
            if ("facet".equals(field.type())) {
                continue;
            }
            List<String> values = clean(params == null ? null : params.get(field.name()));
            String mode = normalizeMode(first(params, field.name() + "Mode"), field.type());
            if (values.isEmpty() && ("isnull".equals(mode) || "isnotnull".equals(mode))) {
                values = List.of();
            } else if (values.isEmpty()) {
                continue;
            }
            if ("song".equals(tab) && "artist".equals(field.name())
                    && Boolean.parseBoolean(first(params, "includeFeaturedSongs"))
                    && List.of("includes", "excludes").contains(mode)) {
                continue;
            }
            Object value = valueFor(row, field.name());
            if (row instanceof ChartArtistOverviewRowDTO artist) {
                value = artistThresholdValue(artist, field.name(), params, value);
            }
            if ("text".equals(field.type()) && isAppearanceField(field.name())) {
                value = chartAppearanceLabel(row, field.name(), value);
            }
            if (!matches(value, field.type(), values, first(params, field.name() + "To"), mode)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesQuery(Object row, String query) {
        if (query == null || query.isBlank()) {
            return true;
        }
        String needle = query.trim().toLowerCase(Locale.ROOT);
        return contains(valueFor(row, "artistName"), needle)
                || contains(valueFor(row, "songTitle"), needle)
                || contains(valueFor(row, "albumName"), needle);
    }

    private boolean matches(Object value, String type, List<String> values, String upperBound, String mode) {
        if ("isnull".equals(mode)) {
            return value == null || String.valueOf(value).isBlank();
        }
        if ("isnotnull".equals(mode)) {
            return value != null && !String.valueOf(value).isBlank();
        }
        if (value == null) {
            return false;
        }
        if ("number".equals(type) || "artist".equals(type)) {
            if ("artist".equals(type)) {
                boolean found = values.stream().anyMatch(item -> String.valueOf(value).equals(item));
                return "excludes".equals(mode) ? !found : found;
            }
            Double number = asNumber(value);
            if (number == null) return false;
            return matchesNumber(number, values, upperBound, mode);
        }
        if ("date".equals(type)) {
            String date = normalizeDate(String.valueOf(value));
            return matchesComparable(date, values, upperBound, mode);
        }
        if ("boolean".equals(type)) {
            boolean actual = Boolean.parseBoolean(String.valueOf(value));
            boolean found = values.stream().anyMatch(item -> String.valueOf(actual).equalsIgnoreCase(item));
            return "excludes".equals(mode) ? !found : found;
        }
        String actual = String.valueOf(value).toLowerCase(Locale.ROOT);
        boolean found = values.stream().map(item -> item.toLowerCase(Locale.ROOT)).anyMatch(actual::contains);
        return "excludes".equals(mode) ? !found : found;
    }

    private boolean matchesNumber(double actual, List<String> values, String upperBound, String mode) {
        List<Double> numbers = values.stream().map(this::asNumber).filter(Objects::nonNull).toList();
        if (numbers.isEmpty()) return false;
        return switch (mode) {
            case "gte" -> actual >= numbers.getFirst();
            case "lte" -> actual <= numbers.getFirst();
            case "between" -> upperBound == null || asNumber(upperBound) == null
                    ? actual == numbers.getFirst()
                    : actual >= Math.min(numbers.getFirst(), asNumber(upperBound)) && actual <= Math.max(numbers.getFirst(), asNumber(upperBound));
            case "excludes" -> numbers.stream().noneMatch(number -> Double.compare(actual, number) == 0);
            default -> numbers.stream().anyMatch(number -> Double.compare(actual, number) == 0);
        };
    }

    private boolean matchesComparable(String actual, List<String> values, String upperBound, String mode) {
        if (actual == null || actual.isBlank()) return false;
        List<String> normalized = values.stream().map(this::normalizeDate).filter(value -> !value.isBlank()).toList();
        if (normalized.isEmpty()) return false;
        String first = normalized.getFirst();
        return switch (mode) {
            case "gte" -> actual.compareTo(first) >= 0;
            case "lte" -> actual.compareTo(first) <= 0;
            case "between" -> {
                String end = normalizeDate(upperBound);
                yield end.isBlank() ? actual.equals(first)
                        : actual.compareTo(first.compareTo(end) <= 0 ? first : end) >= 0
                        && actual.compareTo(first.compareTo(end) <= 0 ? end : first) <= 0;
            }
            case "excludes" -> normalized.stream().noneMatch(actual::contains);
            default -> normalized.stream().anyMatch(actual::contains);
        };
    }

    private Object valueFor(Object row, String field) {
        if (row instanceof ChartSongOverviewRowDTO value) return chartSongValue(value, field);
        if (row instanceof ChartAlbumOverviewRowDTO value) return chartAlbumValue(value, field);
        if (row instanceof ChartArtistOverviewRowDTO value) return chartArtistValue(value, field);
        if (row instanceof PcOverviewRowDTO value) return pcValue(value, field);
        if (row instanceof BillboardHot100OverviewRowDTO value) return billboardValue(value, field);
        if (row instanceof TrlDebut value) return trlValue(value, field);
        return null;
    }

    private Object artistThresholdValue(ChartArtistOverviewRowDTO row, String field,
                                        MultiValueMap<String, String> params, Object fallback) {
        boolean album = field.equals("chartedAlbumsCount") || field.equals("albumTotalChartSpan");
        int[] values = switch (field) {
            case "chartedSongsCount" -> row.getTopSongCounts();
            case "totalChartSpan" -> row.getTopSongWeeks();
            case "chartedAlbumsCount" -> row.getTopAlbumCounts();
            case "albumTotalChartSpan" -> row.getTopAlbumWeeks();
            default -> null;
        };
        try {
            int threshold = Integer.parseInt(first(params, album ? "topAlbum" : "topSong"));
            return values != null && threshold > 0 && threshold < values.length ? values[threshold] : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private Object chartSongValue(ChartSongOverviewRowDTO row, String field) {
        return switch (field) {
            case "artist" -> row.getArtistId();
            case "artistName" -> row.getArtistName(); case "songTitle" -> row.getSongTitle(); case "albumName" -> row.getAlbumName();
            case "genderClass" -> row.getGenderClass(); case "totalChartSpan" -> row.getTotalChartSpan(); case "peakPosition" -> row.getPeakPosition();
            case "spanAtPeak" -> row.getSpanAtPeak(); case "debutPosition" -> row.getDebutPosition();
            case "firstAppearance" -> displayDate(row.getFirstAppearanceLabel(), row.getFirstAppearanceSortValue());
            case "peakAppearance" -> displayDate(row.getPeakAppearanceLabel(), row.getPeakAppearanceSortValue());
            case "lastAppearance" -> displayDate(row.getLastAppearanceLabel(), row.getLastAppearanceSortValue());
            default -> null;
        };
    }

    private Object chartAlbumValue(ChartAlbumOverviewRowDTO row, String field) {
        return switch (field) {
            case "artist" -> row.getResolvedArtistId();
            case "artistName" -> row.getArtistName(); case "albumName" -> row.getAlbumName(); case "genderClass" -> row.getGenderClass();
            case "chartedSongsCount" -> row.getChartedSongsCount(); case "totalChartSpan" -> row.getTotalChartSpan(); case "highestPeak" -> row.getHighestPeak();
            case "numberOneSongsCount" -> row.getNumberOneSongsCount(); case "totalSpanAtNumberOne" -> row.getTotalSpanAtNumberOne(); case "spanAtPeak" -> row.getSpanAtPeak();
            case "debutPosition" -> row.getDebutPosition();
            case "firstAppearance" -> displayDate(row.getFirstDebutDate(), row.getFirstDebutSortValue());
            case "peakAppearance" -> displayDate(row.getPeakAppearanceDate(), row.getPeakAppearanceSortValue());
            case "lastAppearance" -> displayDate(row.getLastAppearanceDate(), row.getLastAppearanceSortValue());
            default -> null;
        };
    }

    private Object chartArtistValue(ChartArtistOverviewRowDTO row, String field) {
        return switch (field) {
            case "artist" -> row.getResolvedArtistId();
            case "artistName" -> row.getArtistName(); case "genderClass" -> row.getGenderClass(); case "chartedSongsCount" -> row.getChartedSongsCount();
            case "totalChartSpan" -> row.getTotalChartSpan(); case "highestPeak" -> row.getHighestPeak(); case "numberOneSongsCount" -> row.getNumberOneSongsCount();
            case "totalSpanAtNumberOne" -> row.getTotalSpanAtNumberOne(); case "chartedAlbumsCount" -> row.getChartedAlbumsCount();
            case "albumTotalChartSpan" -> row.getAlbumTotalChartSpan(); case "albumHighestPeak" -> row.getAlbumHighestPeak();
            case "numberOneAlbumsCount" -> row.getNumberOneAlbumsCount(); case "albumTotalSpanAtNumberOne" -> row.getAlbumTotalSpanAtNumberOne(); default -> null;
        };
    }

    private Object pcValue(PcOverviewRowDTO row, String field) {
        return switch (field) {
            case "artist" -> row.getResolvedArtistId();
            case "artistName" -> row.getArtistName(); case "songTitle" -> row.getSongTitle(); case "genderClass" -> row.getGenderClass();
            case "totalChartSpan" -> row.getDaysOnCountdown(); case "peakPosition" -> row.getPeakPosition(); case "spanAtPeak" -> row.getDaysAtPeak();
            case "debutPosition" -> row.getDebutPosition(); case "firstAppearance" -> row.getFirstWeek(); case "peakAppearance" -> row.getPeakWeek(); case "lastAppearance" -> row.getLastWeek();
            case "daysAtTop1" -> row.getDaysAtTop1(); case "daysAtTop5" -> row.getDaysAtTop5(); case "daysAtTop10" -> row.getDaysAtTop10(); default -> null;
        };
    }

    private Object billboardValue(BillboardHot100OverviewRowDTO row, String field) {
        return switch (field) {
            case "artist" -> row.getResolvedArtistId();
            case "artistName" -> row.getArtistName(); case "songTitle" -> row.getSongTitle(); case "genderClass" -> row.getGenderClass();
            case "totalChartSpan" -> row.getWeeksOnChart(); case "peakPosition" -> row.getPeakPosition(); case "spanAtPeak" -> row.getWeeksAtPeak();
            case "debutPosition" -> row.getDebutPosition(); case "firstAppearance" -> row.getFirstWeek(); case "peakAppearance" -> row.getPeakWeek(); case "lastAppearance" -> row.getLastWeek();
            case "weeksAtTop1" -> row.getWeeksAtTop1(); case "weeksAtTop5" -> row.getWeeksAtTop5(); case "weeksAtTop10" -> row.getWeeksAtTop10();
            case "weeksAtTop20" -> row.getWeeksAtTop20(); case "weeksAtTop50" -> row.getWeeksAtTop50(); case "weeksAtTop100" -> row.getWeeksAtTop100(); default -> null;
        };
    }

    private Object trlValue(TrlDebut row, String field) {
        return switch (field) {
            case "artist" -> row.getResolvedArtistId();
            case "artistName" -> row.getArtistName(); case "songTitle" -> row.getSongTitle(); case "genderClass" -> row.getGenderClass();
            case "totalChartSpan" -> row.getDaysOnCountdown(); case "actualDays" -> row.getActualDays(); case "peakPosition" -> row.getPeakPosition(); case "spanAtPeak" -> row.getDaysAtPeak();
            case "debutPosition" -> row.getDebutPosition(); case "firstAppearance" -> row.getDebutDate(); case "peakAppearance" -> row.getPeakDate(); case "lastAppearance" -> row.getLastAppearanceDate();
            case "daysAtTop1" -> row.getDaysAtTop1(); case "daysAtTop5" -> row.getDaysAtTop5(); case "daysAtTop10" -> row.getDaysAtTop10(); case "retired" -> row.isRetired(); default -> null;
        };
    }

    private FilterField chartLabel(String source, String tab, FilterField field) {
        if (!List.of("weekly", "seasonal", "yearly").contains(source)) {
            return field;
        }
        String unit = switch (source) {
            case "seasonal" -> "Seasons";
            case "yearly" -> "Years";
            default -> "Weeks";
        };
        String label = switch (field.name()) {
            case "totalChartSpan" -> "artist".equals(tab) ? "Song " + unit : unit;
            case "spanAtPeak" -> unit + " at Peak";
            case "firstAppearance" -> "Debut Date";
            case "peakAppearance" -> "Peak Date";
            case "lastAppearance" -> "Last Date";
            case "highestPeak", "peakPosition" -> "Peak";
            case "chartedSongsCount" -> "Songs Charted";
            case "chartedAlbumsCount" -> "Albums Charted";
            case "albumTotalChartSpan" -> "Album " + unit;
            case "totalSpanAtNumberOne" -> unit + " at #1";
            case "albumTotalSpanAtNumberOne" -> "Album " + unit + " at #1";
            case "numberOneSongsCount" -> "#1 Songs";
            case "numberOneAlbumsCount" -> "#1 Albums";
            default -> field.label();
        };
        String type = !"weekly".equals(source) && isAppearanceField(field.name()) ? "text" : field.type();
        return new FilterField(field.name(), label, type);
    }

    private Object chartAppearanceLabel(Object row, String field, Object fallback) {
        if (row instanceof ChartSongOverviewRowDTO song) {
            return switch (field) {
                case "firstAppearance" -> song.getFirstAppearanceLabel();
                case "peakAppearance" -> song.getPeakAppearanceLabel();
                case "lastAppearance" -> song.getLastAppearanceLabel();
                default -> fallback;
            };
        }
        if (row instanceof ChartAlbumOverviewRowDTO album) {
            return switch (field) {
                case "firstAppearance" -> album.getFirstDebutDate();
                case "peakAppearance" -> album.getPeakAppearanceDate();
                case "lastAppearance" -> album.getLastAppearanceDate();
                default -> fallback;
            };
        }
        return fallback;
    }

    private static boolean isAppearanceField(String field) {
        return List.of("firstAppearance", "peakAppearance", "lastAppearance").contains(field);
    }

    private static FilterField field(String name, String label, String type) { return new FilterField(name, label, type); }
    private static boolean isExternal(String source) { return List.of("pc", "trl", "billboard").contains(source); }
    private static List<FilterField> append(List<FilterField> base, FilterField... added) {
        List<FilterField> result = new ArrayList<>(base);
        result.addAll(List.of(added));
        return result;
    }
    private static String first(MultiValueMap<String, String> params, String key) { return params == null ? null : params.getFirst(key); }
    private static List<String> clean(List<String> values) { return values == null ? List.of() : values.stream().filter(Objects::nonNull).map(String::trim).filter(value -> !value.isBlank()).toList(); }
    private static boolean contains(Object value, String needle) { return value != null && String.valueOf(value).toLowerCase(Locale.ROOT).contains(needle); }
    private static String normalizeMode(String mode, String type) { if (mode == null || mode.isBlank()) return "text".equals(type) || "artist".equals(type) || "gender".equals(type) || "facet".equals(type) ? "includes" : "exact"; return mode.toLowerCase(Locale.ROOT); }
    private Double asNumber(Object value) {
        try {
            double parsed = Double.parseDouble(String.valueOf(value).trim());
            return Double.isFinite(parsed) ? parsed : null;
        } catch (NumberFormatException ignored) { return null; }
    }

    private String displayDate(String label, String fallback) {
        String parsed = normalizeDate(label);
        return parsed.isBlank() ? fallback : parsed;
    }

    private String normalizeDate(String value) {
        if (value == null || value.isBlank()) return "";
        String candidate = value.trim();
        int separator = candidate.indexOf(" - ");
        if (separator >= 0) candidate = candidate.substring(separator + 3).trim();
        try { return LocalDate.parse(candidate, DateTimeFormatter.ofPattern("dd/MM/uuuu").withResolverStyle(ResolverStyle.STRICT)).toString(); } catch (DateTimeParseException ignored) { }
        try { return LocalDate.parse(candidate).toString(); } catch (DateTimeParseException ignored) { }
        for (Locale locale : List.of(Locale.ENGLISH, Locale.getDefault())) {
            try { return LocalDate.parse(candidate, DateTimeFormatter.ofPattern("MMM d, uuuu", locale)).toString(); } catch (DateTimeParseException ignored) { }
        }
        return "";
    }
}
