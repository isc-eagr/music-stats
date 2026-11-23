package library.dto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * One breakdown tile on the weekly chart page (Genre, Ethnicity, Language or Country): every value on the
 * chart with its entry count and share, biggest first, with entries missing the value last as "Unknown".
 */
public record ChartBreakdownDTO(String key, String label, int total, List<Slice> slices) {

    public static final String UNKNOWN = "Unknown";

    public record Slice(String name, int count, int percent) {
    }

    /** Genre, Ethnicity, Language and Country breakdowns of a chart's entries, in that order. */
    public static List<ChartBreakdownDTO> forEntries(List<ChartEntryDTO> entries) {
        return List.of(
            of("genre", "Genre", entries, ChartEntryDTO::getGenreName),
            of("ethnicity", "Ethnicity", entries, ChartEntryDTO::getEthnicityName),
            of("language", "Language", entries, ChartEntryDTO::getLanguageName),
            of("country", "Country", entries, ChartEntryDTO::getCountryName));
    }

    static ChartBreakdownDTO of(String key, String label, List<ChartEntryDTO> entries, Function<ChartEntryDTO, String> value) {
        List<ChartEntryDTO> rows = entries == null ? List.of() : entries;
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (ChartEntryDTO entry : rows) {
            String name = value.apply(entry);
            counts.merge(name == null || name.isBlank() ? UNKNOWN : name.trim(), 1, Integer::sum);
        }
        int total = rows.size();
        List<Slice> slices = new ArrayList<>();
        counts.forEach((name, count) -> slices.add(new Slice(name, count, total == 0 ? 0 : (int) Math.round(count * 100.0 / total))));
        slices.sort(Comparator.comparing((Slice slice) -> UNKNOWN.equals(slice.name()))
            .thenComparing(Slice::count, Comparator.reverseOrder())
            .thenComparing(Slice::name));
        return new ChartBreakdownDTO(key, label, total, List.copyOf(slices));
    }

    /** The biggest known value, or the first slice when everything is unknown; null on an empty chart. */
    public Slice top() {
        return slices.isEmpty() ? null : slices.getFirst();
    }
}
