import library.service.*;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.nio.file.*;
import java.util.*;

/** Read-only round-two benchmark. Starts no Spring context or background jobs. */
public class CatalogPathBenchmark {
    record Case(String path, Object service, String suffix, String name, String sort, String direction) {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("Usage: <SQLite file> <output prefix> [runs=2] [path substring]");
        var source = new SingleConnectionDataSource("jdbc:sqlite:" + Path.of(args[0]).toUri() + "?mode=ro", true);
        try {
            var jdbc = new TimedJdbc(source);
            jdbc.execute("PRAGMA query_only=ON");
            jdbc.execute("PRAGMA busy_timeout=10000");
            var winning = new CatalogWinningPeriodService(jdbc);
            var languages = new LanguageService(null, null, jdbc, winning);
            var countries = new CountryService(jdbc, winning);
            var genres = new GenreService(null, null, jdbc, winning);
            var ethnicities = new EthnicityService(null, null, jdbc, winning);
            var genders = new GenderService(null, jdbc, winning);
            var cases = List.of(
                    new Case("/languages", languages, "Languages", null, "name", "asc"),
                    new Case("/countries", countries, "Countries", null, "name", "asc"),
                    new Case("/genres", genres, "Genres", null, "name", "asc"),
                    new Case("/countries?sortby=plays&sortdir=desc", countries, "Countries", null, "plays", "desc"),
                    new Case("/ethnicities", ethnicities, "Ethnicities", null, "name", "asc"),
                    new Case("/countries?sortby=maleplaypct&sortdir=desc", countries, "Countries", null, "maleplaypct", "desc"),
                    new Case("/languages?sortby=winningdays&sortdir=desc", languages, "Languages", null, "winningdays", "desc"),
                    new Case("/countries?sortby=winningdays&sortdir=desc", countries, "Countries", null, "winningdays", "desc"),
                    new Case("/languages?sortby=plays&sortdir=desc", languages, "Languages", null, "plays", "desc"),
                    new Case("/languages?sortby=maleplaypct&sortdir=desc", languages, "Languages", null, "maleplaypct", "desc"),
                    new Case("/languages?q=English", languages, "Languages", "English", "name", "asc"),
                    new Case("/countries?q=United%20States", countries, "Countries", "United States", "name", "asc"),
                    new Case("/genres?q=rock", genres, "Genres", "rock", "name", "asc"),
                    new Case("/ethnicities?q=Asian", ethnicities, "Ethnicities", "Asian", "name", "asc"),
                    new Case("/genres?sortby=winningdays&sortdir=desc", genres, "Genres", null, "winningdays", "desc"),
                    new Case("/ethnicities?sortby=maleplaypct&sortdir=desc", ethnicities, "Ethnicities", null, "maleplaypct", "desc"),
                    new Case("/genders", genders, "Genders", null, "name", "asc"),
                    new Case("/languages?q=__no_matching_language__", languages, "Languages", "__no_matching_language__", "name", "asc"),
                    new Case("/countries?sortby=random&randomSeed=42", countries, "Countries", null, "random", "asc"),
                    new Case("/countries?sortby=maleplaypct&sortdir=asc", countries, "Countries", null, "maleplaypct", "asc"));
            var timings = new StringBuilder("run\tseconds\tpath\n");
            var stages = new StringBuilder("run\tpath\tseconds\tquery\n");
            int runs = args.length > 2 ? Integer.parseInt(args[2]) : 2;
            for (int run = 1; run <= runs; run++) {
                for (int index = 0; index < cases.size(); index++) {
                    if (index >= 10 && run > 1) continue; // Extra shared-code coverage: one sample.
                    var c = cases.get(index);
                    if (args.length > 3 && !c.path.contains(args[3])) continue;
                    jdbc.stages.clear();
                    long start = System.nanoTime();
                    var cards = c.service.getClass().getMethod("get" + c.suffix, String.class, String.class, String.class, Integer.class)
                            .invoke(c.service, c.name, c.sort, c.direction, 42);
                    var count = c.service.getClass().getMethod("count" + c.suffix, String.class).invoke(c.service, c.name);
                    double seconds = (System.nanoTime() - start) / 1e9;
                    String line = String.format(Locale.ROOT, "%d\t%.3f\t%s%n", run, seconds, c.path);
                    System.out.print(line);
                    timings.append(line);
                    for (String stage : jdbc.stages) stages.append(run).append('\t').append(c.path).append('\t').append(stage).append('\n');
                    Files.writeString(Path.of(args[1] + "-timings.tsv"), timings);
                    Files.writeString(Path.of(args[1] + "-stages.tsv"), stages);
                    Files.writeString(Path.of(args[1] + "-" + index + "-results.txt"), SlowPathBenchmark.snapshot(List.of(cards, count)));
                }
            }
        } finally {
            source.destroy();
        }
    }

    private static class TimedJdbc extends JdbcTemplate {
        final List<String> stages = new ArrayList<>();
        TimedJdbc(SingleConnectionDataSource source) { super(source); }
        private void record(String sql, long start) {
            String label = sql.contains("winning_periods AS") ? "winning periods" : sql.strip().replaceAll("\\s+", " ");
            stages.add(String.format(Locale.ROOT, "%.3f\t%s", (System.nanoTime() - start) / 1e9, label.substring(0, Math.min(100, label.length()))));
        }
        @Override public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
            long start = System.nanoTime();
            try { return super.query(sql, mapper, args); } finally { record(sql, start); }
        }
        @Override public void query(String sql, RowCallbackHandler handler, Object... args) {
            long start = System.nanoTime();
            try { super.query(sql, handler, args); } finally { record(sql, start); }
        }
    }
}
