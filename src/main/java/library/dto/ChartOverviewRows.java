package library.dto;

import java.util.List;

/**
 * Chart-agnostic rows for the shared Chart Overview page (Weekly, Hot 100, TRL, Vato's Cuntdown).
 * Dates are ISO (yyyy-MM-dd) so they sort as text; the *Key fields are what chart links need
 * (a week key such as 2019-W23 on the weekly chart, the chart date everywhere else).
 */
public final class ChartOverviewRows {

    private ChartOverviewRows() {
    }

    /** One song's run on a chart. {@code tiers} holds the span at #1, Top 5, Top 10 (and Top 20 / 50 / 100 on the Hot 100). */
    public record SongRow(
            String runKey,
            Integer songId,
            Integer albumId,
            Integer artistId,
            String title,
            String artistName,
            String albumName,
            String gender,
            boolean matched,
            String coverUrl,
            String coverHoverUrl,
            int span,
            Integer actualSpan,
            Integer peak,
            int spanAtPeak,
            Integer debutPosition,
            String debutDate,
            String debutKey,
            String peakDate,
            String peakKey,
            String lastDate,
            String lastKey,
            int[] tiers,
            boolean retired,
            int rawVariantCount,
            List<String> linkedSongTitles) {
    }

    /** One album's songs (or the album itself, on the weekly album chart) on a chart. */
    public record AlbumRow(
            Integer albumId,
            Integer artistId,
            String albumName,
            String artistName,
            String gender,
            String coverUrl,
            int songs,
            int span,
            Integer peak,
            int spanAtPeak,
            int numberOnes,
            int spanAtNumberOne,
            Integer debutPosition,
            String debutDate,
            String debutKey,
            String peakDate,
            String peakKey,
            String lastDate,
            String lastKey,
            List<String> numberOneTitles) {
    }

    /** One artist's totals on a chart, song side and album side. */
    public record ArtistRow(
            Integer artistId,
            String artistName,
            String gender,
            boolean matched,
            String coverUrl,
            int songs,
            int span,
            Integer peak,
            int numberOnes,
            int spanAtNumberOne,
            int albums,
            int albumSpan,
            Integer albumPeak,
            int albumNumberOnes,
            int albumSpanAtNumberOne,
            String debutDate,
            String debutKey,
            String lastDate,
            String lastKey,
            List<String> numberOneSongTitles,
            List<String> numberOneAlbumTitles) {
    }
}
