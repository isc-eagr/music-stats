import library.controller.*;
import library.repository.*;
import library.service.*;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ValueConstants;

import javax.sql.DataSource;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;

/** Profiles complete controller models without a server or Spring lifecycle, on a read-only database. */
public class DetailPathBenchmark {
    record Case(String path, Object target, String method, Map<String, Object> values) {}
    public static void main(String[] args) throws Exception {
        var source = new SingleConnectionDataSource("jdbc:sqlite:" + Path.of(args[0]).toUri() + "?mode=ro", true);
        try {
            var jdbc = new TimedJdbc(source);
            jdbc.execute("PRAGMA query_only=ON");
            jdbc.execute("PRAGMA busy_timeout=10000");
            var config = new AppConfigService(jdbc, false, "", "", 10, 20, 7, 23);
            var lookup = new LookupRepository(jdbc);
            var library = new iTunesLibraryService();
            var itunes = new ItunesService(jdbc, library);
            var links = new SongLinkService(jdbc);
            var full = new AlbumFullListenCalculator(jdbc, config);
            var artists = new ArtistService(null, null, lookup, jdbc, itunes, links);
            var albums = new AlbumService(new AlbumRepository(jdbc, full), null, lookup, jdbc, itunes);
            var songs = new SongService(new SongRepository(jdbc), null, lookup, jdbc, itunes, config, links);
            var charts = new ChartService(null, null, jdbc, itunes, config, links);
            var bb = new BillboardHot100Service(jdbc, source);
            var pc = new PcService(jdbc);
            var trl = new TrlService(jdbc);
            var tags = new TagService(jdbc);
            var heatmap = new DetailPlayHeatmapService(jdbc);
            var ac = new ArtistController(artists, charts, lookup, itunes, null, config, bb, pc, trl, null, null, tags, heatmap);
            var alc = new AlbumController(albums, charts, artists, lookup, itunes, config, bb, pc, trl, null, null, tags, heatmap);
            var sc = new SongController(songs, charts, artists, albums, library, lookup, config, itunes, trl, pc, bb, links, tags, heatmap);
            var yc = new YearController(new YearService(jdbc));
            var cases = new ArrayList<Case>();
            cases.add(new Case("/songs?trackNumber=1&trackNumberMode=equals", sc, "listSongs", Map.of("trackNumber", 1, "trackNumberMode", "equals")));
            for (String kind : List.of("listen", "release")) {
                for (String sort : List.of("year", "plays", "primary_plays", "legacy_plays", "maleplaypct", "random")) {
                    cases.add(new Case("/" + kind + "-years" + (sort.equals("year") ? "" : "?sortby=" + sort + "&sortdir=desc"), yc,
                            kind.equals("listen") ? "listListenYears" : "listReleaseYears", Map.of("sortby", sort, "randomSeed", 42)));
                }
            }
            for (String kind : List.of("artist", "album", "song")) {
                String key = kind.equals("song") ? "s.id" : "s." + kind + "_id";
                var ids = jdbc.queryForList("SELECT " + key + " FROM Song s JOIN (SELECT song_id,COUNT(*) n FROM Play GROUP BY song_id) p ON p.song_id=s.id WHERE " + key + " IS NOT NULL GROUP BY " + key + " ORDER BY SUM(p.n) DESC LIMIT 2", Integer.class);
                for (Integer id : ids) {
                    cases.add(new Case("/" + kind + "s/" + id, kind.equals("artist") ? ac : kind.equals("album") ? alc : sc,
                            "view" + Character.toUpperCase(kind.charAt(0)) + kind.substring(1), Map.of("id", id)));
                    if (kind.equals("artist")) cases.add(new Case("/artists/" + id + "?includeGroups=true&includeFeatured=true", ac, "viewArtist",
                            Map.of("id", id, "includeGroups", true, "includeFeatured", true)));
                }
            }
            for (int id : List.of(446, 124)) cases.add(new Case("/artists/" + id, ac, "viewArtist", Map.of("id", id)));
            Integer member = jdbc.queryForObject("SELECT member_artist_id FROM ArtistMember ORDER BY member_artist_id LIMIT 1", Integer.class);
            for (boolean main : List.of(true, false)) {
                cases.add(new Case("/artists/" + member + "?includeGroups=true&includeFeatured=true&includeMain=" + main,
                        ac, "viewArtist", Map.of("id", member, "includeMain", main, "includeGroups", true, "includeFeatured", true)));
            }
            cases.add(new Case("/artists/2071?includeMain=false&includeFeatured=true", ac, "viewArtist",
                    Map.of("id", 2071, "includeMain", false, "includeFeatured", true)));
            cases.add(new Case("/albums/1088", alc, "viewAlbum", Map.of("id", 1088)));
            for (int id : List.of(8829, 1570)) cases.add(new Case("/songs/" + id, sc, "viewSong", Map.of("id", id)));
            for (String kind : List.of("artist", "album", "song")) {
                var emptyIds = jdbc.queryForList("SELECT id FROM " + kind + " WHERE id NOT IN (SELECT "
                        + (kind.equals("song") ? "s.id" : "s." + kind + "_id")
                        + " FROM Song s JOIN Play p ON p.song_id=s.id WHERE "
                        + (kind.equals("song") ? "s.id" : "s." + kind + "_id") + " IS NOT NULL) ORDER BY id LIMIT 1", Integer.class);
                if (emptyIds.isEmpty()) continue;
                Integer emptyId = emptyIds.getFirst();
                cases.add(new Case("/" + kind + "s/" + emptyId, kind.equals("artist") ? ac : kind.equals("album") ? alc : sc,
                        "view" + Character.toUpperCase(kind.charAt(0)) + kind.substring(1), Map.of("id", emptyId)));
            }
            cases.add(new Case("/release-years?sortby=maleplaypct&sortdir=asc", yc, "listReleaseYears", Map.of("sortby", "maleplaypct", "sortdir", "asc")));
            itunes.artistExistsInItunes(""); // Exclude one-time XML cache initialization from both versions.
            var timings = new StringBuilder("run\tseconds\tpath\n");
            var stages = new StringBuilder("run\tpath\tseconds\tcaller\tsql\n");
            for (int run = 1; run <= Integer.parseInt(args[2]); run++) {
                for (int i = 0; i < cases.size(); i++) {
                    var c = cases.get(i);
                    if (args.length > 3 && !(args[3].equals("details") ? c.path.matches("/(artists|albums|songs)/.*")
                            : args[3].equals("extended") ? i >= 21 : c.path.contains(args[3]))) continue;
                    var method = Arrays.stream(c.target.getClass().getMethods()).filter(m -> m.getName().equals(c.method)).findFirst().orElseThrow();
                    var model = new ExtendedModelMap();
                    Object[] values = Arrays.stream(method.getParameters()).map(p -> argument(p, c.values, model)).toArray();
                    jdbc.stages.clear();
                    long start = System.nanoTime();
                    method.invoke(c.target, values);
                    double seconds = (System.nanoTime() - start) / 1e9;
                    String line = String.format(Locale.ROOT, "%d\t%.3f\t%s%n", run, seconds, c.path);
                    System.out.print(line);
                    timings.append(line);
                    for (String stage : jdbc.stages) stages.append(run).append('\t').append(c.path).append('\t').append(stage).append('\n');
                    Files.writeString(Path.of(args[1] + "-timings.tsv"), timings);
                    Files.writeString(Path.of(args[1] + "-stages.tsv"), stages);
                    Files.writeString(Path.of(args[1] + "-" + i + "-results.txt"), SlowPathBenchmark.snapshot(model.asMap()));
                }
            }
        } finally { source.destroy(); }
    }
    private static Object argument(Parameter p, Map<String,Object> values, Model model) {
        if (values.containsKey(p.getName())) return values.get(p.getName());
        if (Model.class.isAssignableFrom(p.getType())) return model;
        if (p.getType() == jakarta.servlet.http.HttpServletRequest.class) {
            return Proxy.newProxyInstance(p.getType().getClassLoader(), new Class<?>[]{p.getType()}, (proxy, method, args) -> {
                if (method.getName().equals("getParameterMap")) {
                    var parameters = new HashMap<String, String[]>();
                    values.forEach((key, value) -> parameters.put(key, new String[]{value.toString()}));
                    return parameters;
                }
                throw new UnsupportedOperationException(method.getName());
            });
        }
        var annotation = p.getAnnotation(RequestParam.class);
        if (annotation != null && !annotation.defaultValue().equals(ValueConstants.DEFAULT_NONE)) {
            String value = annotation.defaultValue();
            if (p.getType() == String.class) return value;
            if (p.getType() == Integer.class || p.getType() == int.class) return Integer.valueOf(value);
            if (p.getType() == Boolean.class || p.getType() == boolean.class) return Boolean.valueOf(value);
        }
        return p.getType() == int.class ? 0 : p.getType() == boolean.class ? false : null;
    }
    private static class TimedJdbc extends JdbcTemplate {
        final List<String> stages = new ArrayList<>();
        TimedJdbc(DataSource source) { super(source); }
        private void record(String sql, long start) {
            String caller = Arrays.stream(Thread.currentThread().getStackTrace()).filter(f -> f.getClassName().startsWith("library.")).findFirst().map(f -> f.getClassName() + "." + f.getMethodName() + ":" + f.getLineNumber()).orElse("unknown");
            stages.add(String.format(Locale.ROOT, "%.3f\t%s\t%s", (System.nanoTime()-start)/1e9, caller, sql.replaceAll("\\s+", " ")));
        }
        @Override public <T> T query(String sql, PreparedStatementSetter setter, ResultSetExtractor<T> extractor) {
            long start = System.nanoTime();
            try { return super.query(sql, setter, extractor); } finally { record(sql, start); }
        }
        @Override public <T> T query(String sql, ResultSetExtractor<T> extractor) {
            long start = System.nanoTime();
            try { return super.query(sql, extractor); } finally { record(sql, start); }
        }
    }
}
