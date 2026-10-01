package library.util;

import library.dto.ChartAlbumOverviewRowDTO;
import library.dto.ChartArtistOverviewRowDTO;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Shared tab, sorting and paging helpers for the historical chart overview pages
 * (Billboard Hot 100, Vato's Cuntdown, TRL).
 */
public final class ChartOverviewSupport {

    private ChartOverviewSupport() {
    }

    public static String normalizeOverviewTab(String overviewTab) {
        if ("album".equalsIgnoreCase(overviewTab)) {
            return "album";
        }
        if ("artist".equalsIgnoreCase(overviewTab)) {
            return "artist";
        }
        return "song";
    }

    public static String normalizeDir(String overviewTab, String dir) {
        if (dir == null || dir.isBlank()) {
            if ("song".equals(overviewTab)) {
                return "asc";
            }
            return "desc";
        }
        return "asc".equalsIgnoreCase(dir) ? "asc" : "desc";
    }

    public static <T> List<T> paginateRows(List<T> rows, int page, int size) {
        int fromIndex = Math.max(0, (page - 1) * size);
        if (fromIndex >= rows.size()) {
            return List.of();
        }
        int toIndex = Math.min(rows.size(), fromIndex + size);
        return rows.subList(fromIndex, toIndex);
    }

    public static List<ChartAlbumOverviewRowDTO> sortAlbumRows(List<ChartAlbumOverviewRowDTO> rows, String sort, String dir) {
        List<ChartAlbumOverviewRowDTO> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(buildAlbumComparator(sort, dir));
        return sortedRows;
    }

    public static List<ChartArtistOverviewRowDTO> sortArtistRows(List<ChartArtistOverviewRowDTO> rows, String sort, String dir) {
        List<ChartArtistOverviewRowDTO> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(buildArtistComparator(sort, dir));
        return sortedRows;
    }

    private static Comparator<ChartAlbumOverviewRowDTO> buildAlbumComparator(String sort, String dir) {
        Comparator<ChartAlbumOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "album" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getAlbumName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "songs" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getNumberOneSongsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalSpanAtNumberOne);
            case "firstDebut" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getFirstDebutDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "lastAppearance" -> Comparator.comparing(ChartAlbumOverviewRowDTO::getLastAppearanceDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            default -> Comparator.comparingInt(ChartAlbumOverviewRowDTO::getTotalChartSpan);
        };
        comparator = comparator
            .thenComparing(ChartAlbumOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER))
            .thenComparing(ChartAlbumOverviewRowDTO::getAlbumName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        return "asc".equals(dir) ? comparator : comparator.reversed();
    }

    private static Comparator<ChartArtistOverviewRowDTO> buildArtistComparator(String sort, String dir) {
        Comparator<ChartArtistOverviewRowDTO> comparator = switch (sort) {
            case "artist" -> Comparator.comparing(ChartArtistOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "songs" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getChartedSongsCount);
            case "peak" -> Comparator.comparing(ChartArtistOverviewRowDTO::getHighestPeak, Comparator.nullsLast(Integer::compareTo));
            case "numberOnes" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getNumberOneSongsCount);
            case "atNumberOne" -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalSpanAtNumberOne);
            case "firstDebut" -> Comparator.comparing(ChartArtistOverviewRowDTO::getFirstDebutDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            case "lastAppearance" -> Comparator.comparing(ChartArtistOverviewRowDTO::getLastAppearanceDate, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
            default -> Comparator.comparingInt(ChartArtistOverviewRowDTO::getTotalChartSpan);
        };
        comparator = comparator.thenComparing(ChartArtistOverviewRowDTO::getArtistName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
        return "asc".equals(dir) ? comparator : comparator.reversed();
    }
}
