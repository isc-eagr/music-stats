package library.dto;

import java.util.List;

/**
 * Chart History tab of the artist, album and song detail pages: one tile per chart (the switcher),
 * the "Where they peaked" spread, a card grid per run chart (Weekly, Hot 100, Cuntdown, TRL) and the
 * Seasonal + Yearly calendar. Built by DetailChartHistoryService from data the controllers already load.
 */
public record ChartHistoryViewDTO(
        List<Tile> tiles,
        List<SpreadRow> spread,
        List<SpreadRow> periodSpread,
        List<HitSection> sections,
        Calendar calendar) {

    /** Where they peaked has rows (run charts and/or the seasonal + yearly group). */
    public boolean hasSpread() {
        return !spread.isEmpty() || !periodSpread.isEmpty();
    }

    public boolean isEmpty() {
        return tiles.isEmpty();
    }

    /** Target of the first tile, the section shown when the tab opens. */
    public String firstTarget() {
        return tiles.isEmpty() ? null : tiles.get(0).target();
    }

    /**
     * One chart tile. tone: weekly, bb, pc or trl (identity dot); target: the section it opens
     * (weekly, bb, pc, trl or calendar; Seasonal and Yearly both open the calendar).
     * bestCount / bestLabel: time spent at the best peak ("15" with "weeks at #1"); stats: the headline counts.
     */
    public record Tile(String key, String name, String tone, int bestPeak, int bestCount, String bestLabel,
                       List<TileStat> stats, String target) {
        public boolean isNumberOne() {
            return bestPeak == 1;
        }

        /** Grid columns for the tile row: one per tile up to four, then rows of three. */
        public static int columns(int tiles) {
            return tiles <= 4 ? Math.max(tiles, 1) : 3;
        }
    }

    /** One headline count on a tile ("51" over "songs"). */
    public record TileStat(String value, String label) {
    }

    /**
     * One "Where they peaked" row: songs or albums counted by their best position, per band
     * (#1, #2-#5, #6-#10, #11 and below). periodSpread rows use the best seasonal / yearly placement.
     */
    public record SpreadRow(String name, String tone, List<Segment> segments, int total) {
    }

    /** band: one, top5, top10 or rest. */
    public record Segment(String band, int count) {
    }

    /**
     * Card grid of one run chart. unit: "wks" or "days"; songScale / albumScale: the chart's size, used to
     * draw the run bars; periodHref: link pattern for a run box, "{key}" replaced by the run's period key.
     */
    public record HitSection(String key, String title, String unit, int songScale, int albumScale,
                             String periodHref, List<HitCard> songs, List<HitCard> albums) {
        public boolean hasBoth() {
            return !songs.isEmpty() && !albums.isEmpty();
        }

        public String firstView() {
            return songs.isEmpty() ? "albums" : "songs";
        }
    }

    /**
     * One charted song or album. sub / subHref: secondary line (feat. on, by group, or the release date; none for songs on the album page);
     * coverUrl / fallbackUrl: thumbnail and its fallback (null when there is none); runUrl: JSON run endpoint.
     */
    public record HitCard(String kind, Integer id, String name, String href, String sub, String subHref,
                          String coverUrl, String fallbackUrl, String initials,
                          int peak, int total, Integer atPeak,
                          String debutLabel, String debutHref, String peakLabel, String peakHref,
                          String runUrl, Boolean inItunes, List<String> breakdown) {
        public boolean isNumberOne() {
            return peak == 1;
        }

        public boolean hasBreakdown() {
            return breakdown != null && !breakdown.isEmpty();
        }
    }

    /** Seasonal + yearly calendar, one grid per view (songs / albums). */
    public record Calendar(CalendarGrid songs, CalendarGrid albums) {
        public boolean isPresent() {
            return songs != null || albums != null;
        }

        public boolean hasBoth() {
            return songs != null && albums != null;
        }

        public String firstView() {
            return songs != null ? "songs" : "albums";
        }
    }

    /** selectedId: the cell selected when the view opens (best placement, most recent on ties). */
    public record CalendarGrid(String view, List<CalendarYear> years, String selectedId,
                               int seasonsCharted, int numberOnes) {
    }

    /** Five cells: Winter, Spring, Summer, Fall, Year-end. */
    public record CalendarYear(int year, List<CalendarCell> cells) {
    }

    /**
     * kind: season or year; band: one, top5, top10, rest or none (empty cell); items sorted best first.
     */
    public record CalendarCell(String id, String label, String kind, String chartHref, String band,
                               List<CalendarItem> items) {
        public boolean isCharted() {
            return !items.isEmpty();
        }

        public CalendarItem best() {
            return items.isEmpty() ? null : items.get(0);
        }

        public int more() {
            return Math.max(0, items.size() - 1);
        }
    }

    /** note: "feat. on X" / "by X" when the entry comes from a featured song or a group. */
    public record CalendarItem(String name, String href, int position, String note) {
        public boolean isNumberOne() {
            return position == 1;
        }
    }
}
