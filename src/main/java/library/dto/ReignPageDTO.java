package library.dto;

import java.util.List;

/**
 * Everything the Playground > Reigns tab (/playground?view=reigns&reign=artists|songs|genres) renders.
 * Tones are "male", "female" or "other"; genres take the tone of whichever gender has more of their plays.
 */
public record ReignPageDTO(
        String type,
        String title,
        String theme,
        Current current,
        Stats stats,
        List<Lane> lanes,
        List<Tick> timeTicks,
        List<Tick> changeTicks,
        List<GenderSplit> genderSplits,
        List<SnapshotRow> snapshots,
        List<BoardRow> board) {

    /** One item in one snapshot. Movement is "new", "climbed", "dropped" or null. */
    public record Slot(Integer itemId, String name, String secondary, String tone, int position,
                       int playsAtStart, int playsNow, String movement) {
    }

    /** The current #1 and the two behind it (second and third may be null early on). */
    public record Current(Slot first, Slot second, Slot third, String since, int reignDays, String reignOrdinal,
                          int timesAt1, int daysAt1Total, int gapToSecond, int gapToThird) {
    }

    public record Stats(int snapshots, int crownChanges, int holders, int entrants,
                        int longestDays, String longestName, String longestTone,
                        int maleAt1Pct, int maleAt1Days, String since, String spanText) {
    }

    /**
     * One horizontal lane per top 3 spot, laid out twice: on real time, and with every lineup the same width.
     * Left and width are percentages of the lane.
     */
    public record Lane(int position, List<Segment> timeSegments, List<Segment> changeSegments) {
    }

    /**
     * One block in a lane. A shuffle block stands for several stints too thin to read on their own
     * (count of them, names lists who); its tone is "other".
     */
    public record Segment(String name, String tone, double left, double width, String label, boolean showDays,
                          int days, String start, String end, boolean shuffle, int count, String names) {
    }

    public record Tick(String label, double left) {
    }

    /** Share of the days spent at one spot by male and female items (genres weighted by their plays split). */
    public record GenderSplit(int position, int malePct, int femalePct, int maleDays, int femaleDays) {
    }

    public record SnapshotRow(int rank, String start, String end, int days, double barPct, boolean current,
                              List<Slot> slots) {
    }

    /** Top 3 history: every item that reached the top 3. malePct / femalePct are set for genres only. */
    public record BoardRow(int rank, Integer itemId, String name, String secondary, String tone,
                           Integer malePct, Integer femalePct, int best,
                           int daysAt1, int daysAt2, int daysAt3, int daysInTop3,
                           double barAt1, double barAt2, double barAt3,
                           int reignsAt1, int longestAt1, String first, String last, String lastSort,
                           boolean inTop3Now) {
    }
}
