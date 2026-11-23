package library.service;

import library.dto.ReignPageDTO;
import library.dto.ReignPageDTO.BoardRow;
import library.dto.ReignPageDTO.Current;
import library.dto.ReignPageDTO.GenderSplit;
import library.dto.ReignPageDTO.Lane;
import library.dto.ReignPageDTO.Segment;
import library.dto.ReignPageDTO.Slot;
import library.dto.ReignPageDTO.SnapshotRow;
import library.dto.ReignPageDTO.Stats;
import library.dto.ReignPageDTO.Tick;
import library.dto.TopPlayedSnapshotDTO;
import library.dto.TopPlayedSnapshotItemDTO;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns the top 3 snapshots of TopPlayedTimelineService into the Reigns page: the current #1 hero,
 * reigns at #1 (a reign lasts until another item takes #1, however #2 and #3 shuffle), one lane per spot,
 * the gender split of each spot and the Top 3 history table.
 */
public final class ReignPageBuilder {

    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final String PRESENT = "Present";
    /** On the real-time lanes, runs narrower than this (percent of the lane) fold into a striped swaps block. */
    private static final double MIN_READABLE_WIDTH = 1.1;
    private static final int FEMALE_ID = 1;
    private static final int MALE_ID = 2;

    /** All-time plays of one genre split by gender. */
    public record GenreShare(int male, int female, int total) {
        int malePct() { return pct(male, total); }
        int femalePct() { return pct(female, total); }
    }

    private ReignPageBuilder() {
    }

    public static ReignPageDTO build(String type, String title, List<TopPlayedSnapshotDTO> timeline,
                                     Map<Integer, GenreShare> genreShares, LocalDate today) {
        boolean genre = "genre".equals(type);
        Map<Integer, GenreShare> shares = genreShares != null ? genreShares : Map.of();
        List<TopPlayedSnapshotDTO> snaps = timeline == null ? List.of()
                : timeline.stream().filter(s -> s.getItems() != null && !s.getItems().isEmpty()).toList();
        if (snaps.isEmpty()) {
            return new ReignPageDTO(type, title, "", null, null, List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
        }

        LocalDate t0 = parse(snaps.getFirst().getStartDate(), today);
        double span = Math.max(1, ChronoUnit.DAYS.between(t0, today));
        int maxDays = snaps.stream().mapToInt(TopPlayedSnapshotDTO::getDaysInConfig).max().orElse(1);

        List<List<Slot>> slotsBySnap = new ArrayList<>();
        for (TopPlayedSnapshotDTO s : snaps) {
            List<Slot> slots = new ArrayList<>();
            for (TopPlayedSnapshotItemDTO it : s.getItems()) {
                slots.add(new Slot(it.getItemId(), it.getItemName(), it.getSecondaryName(),
                        tone(genre, it, shares), it.getPosition(), it.getPlaysAtSnapshotStart(), it.getPlaysCount(),
                        it.getMovement() == null ? null : it.getMovement().toLowerCase(Locale.ROOT)));
            }
            slotsBySnap.add(slots);
        }

        // Reigns at #1
        List<Reign> reigns = new ArrayList<>();
        for (int i = 0; i < snaps.size(); i++) {
            TopPlayedSnapshotDTO s = snaps.get(i);
            Slot top = slotsBySnap.get(i).getFirst();
            Reign last = reigns.isEmpty() ? null : reigns.getLast();
            if (last == null || !last.itemId.equals(top.itemId())) {
                last = new Reign(top, s.getStartDate());
                reigns.add(last);
            }
            last.days += s.getDaysInConfig();
        }

        // Top 3 history
        Map<Integer, Entry> entries = new LinkedHashMap<>();
        for (int i = 0; i < snaps.size(); i++) {
            TopPlayedSnapshotDTO s = snaps.get(i);
            for (Slot slot : slotsBySnap.get(i)) {
                Entry e = entries.computeIfAbsent(slot.itemId(), id -> new Entry(slot, s.getStartDate()));
                int p = Math.min(Math.max(slot.position(), 1), 3) - 1;
                e.days[p] += s.getDaysInConfig();
                e.best = Math.min(e.best, slot.position());
                e.last = s.isCurrent() ? PRESENT : s.getEndDate();
            }
        }
        for (Reign r : reigns) {
            Entry e = entries.get(r.itemId);
            e.reigns++;
            e.longest = Math.max(e.longest, r.days);
        }
        List<Entry> ranked = new ArrayList<>(entries.values());
        ranked.sort(Comparator.<Entry>comparingInt(e -> -e.days[0])
                .thenComparingInt(e -> -e.days[1])
                .thenComparingInt(e -> -e.days[2]));
        List<Slot> currentSlots = slotsBySnap.getLast();
        int maxTop3 = Math.max(1, ranked.stream().mapToInt(Entry::total).max().orElse(1));
        List<BoardRow> board = new ArrayList<>();
        for (int i = 0; i < ranked.size(); i++) {
            Entry e = ranked.get(i);
            GenreShare share = genre ? shares.get(e.slot.itemId()) : null;
            boolean now = currentSlots.stream().anyMatch(c -> c.itemId().equals(e.slot.itemId()));
            board.add(new BoardRow(i + 1, e.slot.itemId(), e.slot.name(), e.slot.secondary(), e.slot.tone(),
                    share != null ? share.malePct() : null, share != null ? share.femalePct() : null, e.best,
                    e.days[0], e.days[1], e.days[2], e.total(),
                    bar(e.days[0], maxTop3), bar(e.days[1], maxTop3), bar(e.days[2], maxTop3),
                    e.reigns, e.longest, e.first, e.last, PRESENT.equals(e.last) ? today.format(DMY) : e.last, now));
        }

        // Current #1 and the two behind it
        Reign currentReign = reigns.getLast();
        Slot first = currentSlots.getFirst();
        Slot second = currentSlots.size() > 1 ? currentSlots.get(1) : null;
        Slot third = currentSlots.size() > 2 ? currentSlots.get(2) : null;
        int timesAt1 = entries.get(first.itemId()).reigns;
        Current current = new Current(first, second, third, currentReign.start, currentReign.days, ordinal(timesAt1), timesAt1,
                entries.get(first.itemId()).days[0],
                second != null ? first.playsNow() - second.playsNow() : 0,
                second != null && third != null ? second.playsNow() - third.playsNow() : 0);

        // Gender split of each spot
        double[][] gd = new double[3][2];
        for (int i = 0; i < snaps.size(); i++) {
            int days = snaps.get(i).getDaysInConfig();
            for (Slot slot : slotsBySnap.get(i)) {
                int p = Math.min(Math.max(slot.position(), 1), 3) - 1;
                GenreShare share = genre ? shares.get(slot.itemId()) : null;
                if (share != null) {
                    gd[p][0] += days * (double) share.malePct() / 100;
                    gd[p][1] += days * (double) share.femalePct() / 100;
                } else if ("male".equals(slot.tone())) {
                    gd[p][0] += days;
                } else if ("female".equals(slot.tone())) {
                    gd[p][1] += days;
                }
            }
        }
        List<GenderSplit> genderSplits = new ArrayList<>();
        for (int p = 0; p < 3; p++) {
            double total = 0;
            for (int i = 0; i < snaps.size(); i++) {
                if (slotsBySnap.get(i).size() > p) total += snaps.get(i).getDaysInConfig();
            }
            genderSplits.add(new GenderSplit(p + 1, pct(gd[p][0], total), pct(gd[p][1], total),
                    (int) Math.round(gd[p][0]), (int) Math.round(gd[p][1])));
        }

        // Lanes: one per spot, consecutive snapshots with the same holder merge into one run
        double[] timeX = new double[snaps.size() + 1];
        double[] changeX = new double[snaps.size() + 1];
        for (int i = 0; i < snaps.size(); i++) {
            timeX[i] = ChronoUnit.DAYS.between(t0, parse(snaps.get(i).getStartDate(), today)) / span * 100;
            changeX[i] = i * 100.0 / snaps.size();
        }
        timeX[snaps.size()] = 100;
        changeX[snaps.size()] = 100;
        List<Lane> lanes = new ArrayList<>();
        for (int p = 0; p < 3; p++) {
            List<Run> runs = new ArrayList<>();
            for (int i = 0; i < snaps.size(); i++) {
                List<Slot> slots = slotsBySnap.get(i);
                if (slots.size() <= p) continue;
                Slot slot = slots.get(p);
                TopPlayedSnapshotDTO s = snaps.get(i);
                Run last = runs.isEmpty() ? null : runs.getLast();
                if (last != null && last.slot.itemId().equals(slot.itemId()) && last.i1 == i - 1) {
                    last.i1 = i;
                    last.days += s.getDaysInConfig();
                    last.end = s.getEndDate();
                } else {
                    runs.add(new Run(slot, i, s.getStartDate(), s.getEndDate(), s.getDaysInConfig()));
                }
            }
            lanes.add(new Lane(p + 1, laneSegments(runs, timeX, MIN_READABLE_WIDTH), laneSegments(runs, changeX, 0)));
        }

        List<Tick> timeTicks = new ArrayList<>();
        for (int y = t0.getYear() + 1; y <= today.getYear(); y++) {
            double left = ChronoUnit.DAYS.between(t0, LocalDate.of(y, 1, 1)) / span * 100;
            timeTicks.add(new Tick(yearLabel(y), round2(left)));
        }
        // Every change: a tick where each year's first lineup starts, skipping ticks that would collide
        List<Tick> changeTicks = new ArrayList<>();
        double lastTick = -99;
        for (int y = t0.getYear() + 1; y <= today.getYear(); y++) {
            for (int i = 0; i < snaps.size(); i++) {
                if (parse(snaps.get(i).getStartDate(), today).getYear() >= y) {
                    if (changeX[i] - lastTick >= 2.6) {
                        changeTicks.add(new Tick(yearLabel(y), round2(changeX[i])));
                        lastTick = changeX[i];
                    }
                    break;
                }
            }
        }

        List<SnapshotRow> rows = new ArrayList<>();
        for (int i = snaps.size() - 1; i >= 0; i--) {
            TopPlayedSnapshotDTO s = snaps.get(i);
            rows.add(new SnapshotRow(s.getRank(), s.getStartDate(), s.getEndDate(), s.getDaysInConfig(),
                    round2(Math.max(1, s.getDaysInConfig() * 100.0 / Math.max(1, maxDays))), s.isCurrent(), slotsBySnap.get(i)));
        }

        Reign longest = reigns.stream().max(Comparator.comparingInt(r -> r.days)).orElse(currentReign);
        long holders = reigns.stream().map(r -> r.itemId).distinct().count();
        GenderSplit at1 = genderSplits.getFirst();
        Stats stats = new Stats(snaps.size(), reigns.size() - 1, (int) holders, board.size(),
                longest.days, longest.name, longest.tone, at1.malePct(), at1.maleDays(),
                snaps.getFirst().getStartDate(), String.format(Locale.ROOT, "%.1fy", span / 365.25));

        String theme = switch (first.tone()) {
            case "male" -> "gender-male";
            case "female" -> "gender-female";
            default -> "";
        };
        return new ReignPageDTO(type, title, theme, current, stats, lanes, timeTicks, changeTicks, genderSplits, rows, board);
    }

    /**
     * Blocks for one lane. Runs narrower than minWidth (percent of the lane) fold into one striped "shuffle" block
     * so short stints never pile up as slivers; a lone narrow run keeps its color at a small fixed width.
     * Widened blocks push the next block right instead of overlapping it.
     */
    private static List<Segment> laneSegments(List<Run> runs, double[] x, double minWidth) {
        List<Segment> out = new ArrayList<>();
        List<Run> pending = new ArrayList<>();
        for (Run r : runs) {
            double left = x[r.i0];
            double width = x[r.i1 + 1] - left;
            if (width < minWidth) {
                if (!pending.isEmpty() && pending.getLast().i1 != r.i0 - 1) {
                    flushShuffle(pending, x, minWidth, out);
                }
                pending.add(r);
                continue;
            }
            flushShuffle(pending, x, minWidth, out);
            String label = width >= 4.5 ? r.slot.name() : (width >= 2.2 ? r.slot.name().split(" ")[0] : "");
            out.add(new Segment(r.slot.name(), r.slot.tone(), left, width, label, width >= 9, r.days,
                    r.start, r.end, false, 1, r.slot.name()));
        }
        flushShuffle(pending, x, minWidth, out);

        List<Segment> fixed = new ArrayList<>();
        double prevEnd = 0;
        for (Segment s : out) {
            double left = s.left();
            double width = s.width();
            if (left < prevEnd) {
                width = Math.max(0.3, width - (prevEnd - left));
                left = prevEnd;
            }
            fixed.add(new Segment(s.name(), s.tone(), round2(left), round2(width), s.label(), s.showDays(), s.days(),
                    s.start(), s.end(), s.shuffle(), s.count(), s.names()));
            prevEnd = left + width;
        }
        return fixed;
    }

    private static void flushShuffle(List<Run> pending, double[] x, double minWidth, List<Segment> out) {
        if (pending.isEmpty()) return;
        Run first = pending.getFirst();
        Run last = pending.getLast();
        double left = x[first.i0];
        double width = x[last.i1 + 1] - left;
        int days = pending.stream().mapToInt(r -> r.days).sum();
        if (pending.size() == 1) {
            out.add(new Segment(first.slot.name(), first.slot.tone(), left, Math.max(width, 0.5), "", false, days,
                    first.start, first.end, false, 1, first.slot.name()));
        } else {
            String names = String.join(", ", pending.stream().map(r -> r.slot.name()).distinct().toList());
            String label = Math.max(width, minWidth) >= 2.4 ? pending.size() + " swaps" : String.valueOf(pending.size());
            out.add(new Segment("", "other", left, Math.max(width, minWidth), label, false, days,
                    first.start, last.end, true, pending.size(), names));
        }
        pending.clear();
    }

    private static String yearLabel(int year) {
        return "'" + String.format("%02d", year % 100);
    }

    private static String tone(boolean genre, TopPlayedSnapshotItemDTO it, Map<Integer, GenreShare> shares) {
        if (genre) {
            GenreShare share = shares.get(it.getItemId());
            if (share == null || share.total() == 0) return "other";
            return share.male() >= share.female() ? "male" : "female";
        }
        Integer g = it.getGenderId();
        if (g != null && g == MALE_ID) return "male";
        if (g != null && g == FEMALE_ID) return "female";
        return "other";
    }

    static String ordinal(int n) {
        int mod100 = n % 100;
        String suffix = (mod100 >= 11 && mod100 <= 13) ? "th" : switch (n % 10) {
            case 1 -> "st";
            case 2 -> "nd";
            case 3 -> "rd";
            default -> "th";
        };
        return n + suffix + " reign";
    }

    private static LocalDate parse(String dmy, LocalDate today) {
        if (dmy == null || PRESENT.equals(dmy)) return today;
        try {
            return LocalDate.parse(dmy, DMY);
        } catch (Exception e) {
            return today;
        }
    }

    private static int pct(double part, double total) {
        return total > 0 ? (int) Math.round(part / total * 100) : 0;
    }

    private static double bar(int value, int max) {
        return round2(value * 100.0 / max);
    }

    private static double round2(double v) {
        return Math.round(v * 100) / 100.0;
    }

    private static final class Reign {
        final Integer itemId;
        final String name;
        final String tone;
        final String start;
        int days;

        Reign(Slot top, String start) {
            this.itemId = top.itemId();
            this.name = top.name();
            this.tone = top.tone();
            this.start = start;
        }
    }

    private static final class Entry {
        final Slot slot;
        final String first;
        final int[] days = new int[3];
        int best = 3;
        String last;
        int reigns;
        int longest;

        Entry(Slot slot, String first) {
            this.slot = slot;
            this.first = first;
        }

        int total() {
            return days[0] + days[1] + days[2];
        }
    }

    /** Consecutive snapshots (i0..i1) with the same item at one spot. */
    private static final class Run {
        final Slot slot;
        final int i0;
        final String start;
        int i1;
        String end;
        int days;

        Run(Slot slot, int i, String start, String end, int days) {
            this.slot = slot;
            this.i0 = i;
            this.i1 = i;
            this.start = start;
            this.end = end;
            this.days = days;
        }
    }
}
