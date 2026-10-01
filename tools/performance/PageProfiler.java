import library.controller.*;
import library.repository.*;
import library.service.*;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.lang.reflect.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

/**
 * Profiles controller handlers for a list of URLs on a read-only database, recording each JDBC
 * statement's time, caller, SQL (JSON-encoded) and bind parameters. Starts no Spring context,
 * server or scheduled jobs. Each URL runs inside a simulated request scope.
 * Args: <db file> <urls file> <output prefix> [reps=2] [extra JDBC URL params, e.g. "&cache_size=-65536"]
 * Writes <prefix>-timings.tsv and <prefix>-stages.tsv. With -Dsnapshots=<dir>, also writes one
 * complete model snapshot per URL so two implementations can be diffed for identical output.
 * With -Dpool=<n>, uses a Hikari pool like the app, which lets concurrent code paths run concurrently.
 */
public class PageProfiler {
    record Route(Pattern pattern, Object target, String method) {}

    public static void main(String[] args) throws Exception {
        String extra = args.length > 4 ? args[4] : "";
        String jdbcUrl = "jdbc:sqlite:" + Path.of(args[0]).toUri() + "?mode=ro" + extra;
        // -Dpool=N uses a Hikari pool like the app (needed for code that runs queries concurrently).
        DataSource source;
        if (System.getProperty("pool") != null) {
            var pool = new com.zaxxer.hikari.HikariDataSource();
            pool.setJdbcUrl(jdbcUrl);
            pool.setMaximumPoolSize(Integer.parseInt(System.getProperty("pool")));
            source = pool;
        } else {
            source = new SingleConnectionDataSource(jdbcUrl, true);
        }
        try {
            var jdbc = new TimedJdbc(source);
            jdbc.execute("PRAGMA query_only=ON");
            jdbc.execute("PRAGMA busy_timeout=10000");
            System.out.println("# cache_size=" + jdbc.queryForObject("PRAGMA cache_size", String.class)
                    + " mmap_size=" + jdbc.queryForObject("PRAGMA mmap_size", String.class)
                    + " temp_store=" + jdbc.queryForObject("PRAGMA temp_store", String.class)
                    + " sqlite=" + jdbc.queryForObject("SELECT sqlite_version()", String.class));
            jdbc.stages.clear();
            var routes = wire(jdbc, source);
            var urls = Files.readAllLines(Path.of(args[1])).stream().map(String::strip).filter(s -> !s.isEmpty() && !s.startsWith("#")).toList();
            int reps = args.length > 3 ? Integer.parseInt(args[3]) : 2;
            var timings = new StringBuilder("url\t" + String.join("\t", java.util.stream.IntStream.rangeClosed(1, reps).mapToObj(i -> "t" + i).toList()) + "\tsql_total\tqueries\terror\n");
            var stages = new StringBuilder("url\trep\tseconds\tcaller\tsql\tparams\n");
            for (String url : urls) {
                double[] t = new double[reps];
                double sqlTotal = 0; int queries = 0; String error = "";
                for (int rep = 0; rep < reps; rep++) {
                    jdbc.stages.clear();
                    long start = System.nanoTime();
                    try {
                        Object[] result = invoke(routes, url);
                        if (rep == 0 && System.getProperty("snapshots") != null) {
                            Path dir = Path.of(System.getProperty("snapshots"));
                            Files.createDirectories(dir);
                            String file = url.replaceAll("[^A-Za-z0-9=&_-]", "_");
                            Files.writeString(dir.resolve(file.substring(0, Math.min(file.length(), 150)) + ".txt"),
                                    snapshot(((ExtendedModelMap) result[1]).asMap()) + "\n=> " + snapshot(result[0]));
                        }
                    } catch (Throwable e) {
                        Throwable root = e; while (root.getCause() != null) root = root.getCause();
                        error = (root.getClass().getSimpleName() + ": " + root.getMessage()).replaceAll("\\s+", " ");
                        error = error.substring(0, Math.min(160, error.length()));
                    }
                    t[rep] = (System.nanoTime() - start) / 1e9;
                    sqlTotal = 0; queries = jdbc.stages.size();
                    for (String[] s : jdbc.stages) {
                        sqlTotal += Double.parseDouble(s[0]);
                        stages.append(url).append('\t').append(rep + 1).append('\t').append(s[0]).append('\t').append(s[1]).append('\t').append(s[2]).append('\n');
                    }
                }
                StringBuilder line = new StringBuilder(url);
                for (double v : t) line.append('\t').append(String.format(Locale.ROOT, "%.3f", v));
                line.append('\t').append(String.format(Locale.ROOT, "%.3f", sqlTotal)).append('\t').append(queries).append('\t').append(error);
                System.out.println(line);
                timings.append(line).append('\n');
                Files.writeString(Path.of(args[2] + "-timings.tsv"), timings);
                Files.writeString(Path.of(args[2] + "-stages.tsv"), stages);
            }
        } finally {
            if (source instanceof SingleConnectionDataSource single) single.destroy();
            if (source instanceof AutoCloseable closeable) closeable.close();
        }
    }

    static List<Route> wire(JdbcTemplate jdbc, DataSource source) throws Exception {
        var config = new AppConfigService(jdbc, false, "", "", 10, 20, 7, 23);
        var lookup = new LookupRepository(jdbc);
        var library = new iTunesLibraryService();
        var itunes = new ItunesService(jdbc, library);
        var links = new SongLinkService(jdbc);
        var full = new AlbumFullListenCalculator(jdbc, config);
        var artists = new ArtistService(artistRepository(jdbc), null, lookup, jdbc, itunes, links);
        var albums = new AlbumService(new AlbumRepository(jdbc, full), null, lookup, jdbc, itunes);
        var songs = new SongService(new SongRepository(jdbc), null, lookup, jdbc, itunes, config, links);
        var charts = new ChartService(chartRepository(jdbc), null, jdbc, itunes, config, links);
        var bb = new BillboardHot100Service(jdbc, source);
        var pc = new PcService(jdbc);
        var trl = new TrlService(jdbc);
        var tags = new TagService(jdbc);
        var heatmap = new DetailPlayHeatmapService(jdbc);
        var catalogCharts = new CatalogChartService(songs);
        var chartFilters = new ChartFilterRequestFactory();
        var winning = new CatalogWinningPeriodService(jdbc);
        var overviewFilters = new OverviewFilterService(new OverviewFacetService(jdbc, lookup));
        var main = new MainController();
        setField(main, "songRepositoryImpl", new SongRepositoryImpl(jdbc));
        setField(main, "globalSearchService", new GlobalSearchService(jdbc));
        itunes.artistExistsInItunes(""); // one-time iTunes XML load, excluded from timings
        var r = new ArrayList<Route>();
        var ac = new ArtistController(artists, charts, lookup, itunes, null, config, bb, pc, trl, catalogCharts, chartFilters, tags, heatmap);
        var alc = new AlbumController(albums, charts, artists, lookup, itunes, config, bb, pc, trl, catalogCharts, chartFilters, tags, heatmap);
        var sc = new SongController(songs, charts, artists, albums, library, lookup, config, itunes, trl, pc, bb, links, tags, heatmap);
        var chc = new ChartsController(charts, config, bb, pc, trl, overviewFilters);
        route(r, "/", main, "index");
        route(r, "/albums", alc, "listAlbums");
        route(r, "/albums/api", alc, "listAlbumsApi");
        route(r, "/albums/(?<id>\\d+)", alc, "viewAlbum");
        route(r, "/artists", ac, "listArtists");
        route(r, "/artists/api", ac, "listArtistsApi");
        route(r, "/artists/(?<id>\\d+)", ac, "viewArtist");
        route(r, "/songs", sc, "listSongs");
        route(r, "/songs/api", sc, "listSongsApi");
        route(r, "/songs/(?<id>\\d+)", sc, "viewSong");
        route(r, "/genres", new GenreController(new GenreService(null, lookup, jdbc, winning)), "listGenres");
        route(r, "/subgenres", new SubGenreController(new SubGenreService(null, lookup, jdbc, winning)), "listSubGenres");
        route(r, "/languages", new LanguageController(new LanguageService(null, lookup, jdbc, winning)), "listLanguages");
        route(r, "/genders", new GenderController(new GenderService(lookup, jdbc, winning)), "listGenders");
        route(r, "/ethnicities", new EthnicityController(new EthnicityService(null, lookup, jdbc, winning)), "listEthnicities");
        route(r, "/countries", new CountryController(new CountryService(jdbc, winning)), "listCountries");
        route(r, "/(?<periodType>days|weeks|months|seasons|years|decades)", new TimeframeController(new TimeframeService(jdbc), charts, lookup, jdbc, config), "listTimeframes");
        var yc = new YearController(new YearService(jdbc));
        route(r, "/listen-years", yc, "listListenYears");
        route(r, "/release-years", yc, "listReleaseYears");
        route(r, "/tags", new TagController(tags), "listTags");
        route(r, "/playground", new PlaygroundController(new PlaygroundService(jdbc)), "playground");
        var tpc = new TopPlayedTimelineController(new TopPlayedTimelineService(jdbc));
        route(r, "/reign/artists", tpc, "artistTimeline");
        route(r, "/reign/songs", tpc, "songTimeline");
        route(r, "/reign/genres", tpc, "genreTimeline");
        route(r, "/charts/weekly", chc, "weeklyCharts");
        route(r, "/charts/weekly/overview", chc, "weeklyOverview");
        route(r, "/charts/seasonal/overview", chc, "seasonalOverview");
        route(r, "/charts/yearly/overview", chc, "yearlyOverview");
        route(r, "/charts/number-ones", chc, "weeklyNumberOnes");
        route(r, "/charts/weekly/(?<periodKey>[^/]+)", chc, "weeklyChart");
        route(r, "/charts/seasonal/(?<periodKey>[^/]+)", chc, "seasonalChart");
        route(r, "/charts/yearly/(?<periodKey>[^/]+)", chc, "yearlyChart");
        route(r, "/misc/trl", new TrlController(config, trl, overviewFilters), "trlList");
        route(r, "/misc/vatos-cuntdown", new PcController(config, pc, overviewFilters), "pcList");
        route(r, "/misc/billboard-hot-100", new BillboardHot100Controller(config, bb, overviewFilters), "overview");
        return r;
    }

    /** Routes ArtistRepositoryCustom methods to the real JDBC implementation. */
    static ArtistRepository artistRepository(JdbcTemplate jdbc) {
        var impl = new ArtistRepositoryImpl(jdbc);
        return (ArtistRepository) Proxy.newProxyInstance(PageProfiler.class.getClassLoader(), new Class<?>[]{ArtistRepository.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == ArtistRepositoryCustom.class) {
                        try { return method.invoke(impl, args); } catch (InvocationTargetException e) { throw e.getCause(); }
                    }
                    return switch (method.getName()) {
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "equals" -> proxy == args[0];
                        case "toString" -> "ProfilerArtistRepository";
                        default -> throw new UnsupportedOperationException("ArtistRepository." + method.getName());
                    };
                });
    }

    /** JDBC-backed stand-in for the few read-only JPA chart lookups used by list pages. */
    static ChartRepository chartRepository(JdbcTemplate jdbc) {
        return (ChartRepository) Proxy.newProxyInstance(PageProfiler.class.getClassLoader(), new Class<?>[]{ChartRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "findFinalizedPeriodKeysByPeriodType" -> new HashSet<>(jdbc.queryForList(
                            "SELECT period_key FROM Chart WHERE period_type = ? AND is_finalized = 1", String.class, args[0]));
                    case "findAllPeriodKeysByPeriodType" -> new HashSet<>(jdbc.queryForList(
                            "SELECT period_key FROM Chart WHERE period_type = ?", String.class, args[0]));
                    case "findAllPeriodKeysByChartType" -> new HashSet<>(jdbc.queryForList(
                            "SELECT period_key FROM Chart WHERE chart_type = ?", String.class, args[0]));
                    case "existsByChartTypeAndPeriodKey" -> jdbc.queryForObject(
                            "SELECT COUNT(*) FROM Chart WHERE chart_type = ? AND period_key = ?", Integer.class, args[0], args[1]) > 0;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "ProfilerChartRepository";
                    default -> throw new UnsupportedOperationException("ChartRepository." + method.getName());
                });
    }

    static void route(List<Route> routes, String regex, Object target, String method) {
        routes.add(new Route(Pattern.compile("^" + regex + "$"), target, method));
    }

    static void setField(Object target, String name, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    /** Deterministic dump of models and DTOs (same approach as tools/performance/SlowPathBenchmark). */
    static String snapshot(Object object) throws Exception {
        if (object == null) return "null";
        if (object instanceof org.springframework.http.ResponseEntity<?> entity) return "ResponseEntity(" + entity.getStatusCode() + ") " + snapshot(entity.getBody());
        if (object instanceof Map<?, ?> map) {
            var sorted = new TreeMap<String, String>();
            for (var entry : map.entrySet()) sorted.put(String.valueOf(entry.getKey()), snapshot(entry.getValue()));
            return sorted.toString();
        }
        if (object instanceof Collection<?> collection) {
            var rows = new ArrayList<String>();
            for (Object item : collection) rows.add(snapshot(item));
            return String.join("\n", rows);
        }
        if (object.getClass().getPackageName().startsWith("library")) {
            var fields = new TreeMap<String, String>();
            for (Method method : object.getClass().getMethods()) {
                if (method.getParameterCount() == 0 && (method.getName().startsWith("get") || method.getName().startsWith("is")) && !method.getName().equals("getClass")) {
                    fields.put(method.getName(), snapshot(method.invoke(object)));
                }
            }
            if (object.getClass().isRecord()) {
                for (var component : object.getClass().getRecordComponents()) fields.put(component.getName(), snapshot(component.getAccessor().invoke(object)));
            }
            return fields.toString();
        }
        return object.toString();
    }

    static Object[] invoke(List<Route> routes, String url) throws Exception {
        String path = url.contains("?") ? url.substring(0, url.indexOf('?')) : url;
        String query = url.contains("?") ? url.substring(url.indexOf('?') + 1) : "";
        Map<String, List<String>> params = new LinkedHashMap<>();
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            String k = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            String v = eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            params.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
        }
        for (Route route : routes) {
            Matcher m = route.pattern.matcher(path);
            if (!m.matches()) continue;
            Map<String, String> pathVars = new HashMap<>();
            for (String g : List.of("id", "periodType", "periodKey")) {
                try { if (m.group(g) != null) pathVars.put(g, m.group(g)); } catch (IllegalArgumentException ignored) { }
            }
            Method method = Arrays.stream(route.target.getClass().getMethods())
                    .filter(x -> x.getName().equals(route.method))
                    .filter(x -> x.isAnnotationPresent(GetMapping.class) || x.isAnnotationPresent(RequestMapping.class))
                    .findFirst().orElseThrow(() -> new IllegalStateException("No handler " + route.method));
            var model = new ExtendedModelMap();
            var req = request(params);
            org.springframework.web.context.request.RequestContextHolder.setRequestAttributes(
                    new org.springframework.web.context.request.ServletRequestAttributes(req));
            try {
                Object[] values = new Object[method.getParameterCount()];
                Parameter[] ps = method.getParameters();
                for (int i = 0; i < ps.length; i++) values[i] = argument(ps[i], params, pathVars, model, req);
                return new Object[]{method.invoke(route.target, values), model};
            } finally {
                org.springframework.web.context.request.RequestContextHolder.resetRequestAttributes();
            }
        }
        throw new IllegalArgumentException("No route for " + path);
    }

    static Object argument(Parameter p, Map<String, List<String>> params, Map<String, String> pathVars, ExtendedModelMap model,
                           jakarta.servlet.http.HttpServletRequest req) {
        Class<?> type = p.getType();
        if (org.springframework.ui.Model.class.isAssignableFrom(type)) return model;
        if (type == jakarta.servlet.http.HttpServletRequest.class) return req;
        String name = p.getName();
        PathVariable pv = p.getAnnotation(PathVariable.class);
        if (pv != null) {
            String n = !pv.value().isEmpty() ? pv.value() : !pv.name().isEmpty() ? pv.name() : name;
            return convert(type, p.getParameterizedType(), pathVars.containsKey(n) ? List.of(pathVars.get(n)) : null);
        }
        RequestParam rp = p.getAnnotation(RequestParam.class);
        if (rp != null) {
            if (!rp.value().isEmpty()) name = rp.value();
            else if (!rp.name().isEmpty()) name = rp.name();
            if (Map.class.isAssignableFrom(type)) {
                Map<String, String> flat = new LinkedHashMap<>();
                params.forEach((k, v) -> flat.put(k, v.getFirst()));
                return flat;
            }
            List<String> raw = params.get(name);
            if (raw == null && !rp.defaultValue().equals(ValueConstants.DEFAULT_NONE)) raw = List.of(rp.defaultValue());
            return convert(type, p.getParameterizedType(), raw);
        }
        return convert(type, p.getParameterizedType(), null);
    }

    static Object convert(Class<?> type, Type generic, List<String> raw) {
        if (raw == null || raw.isEmpty()) {
            if (type == int.class) return 0;
            if (type == long.class) return 0L;
            if (type == double.class) return 0d;
            if (type == boolean.class) return false;
            return null;
        }
        String first = raw.getFirst();
        if (List.class.isAssignableFrom(type)) {
            Type item = generic instanceof ParameterizedType pt ? pt.getActualTypeArguments()[0] : String.class;
            List<Object> out = new ArrayList<>();
            for (String v : raw) for (String part : v.split(",")) {
                if (part.isEmpty()) continue;
                out.add(item == Integer.class ? Integer.valueOf(part) : item == Long.class ? Long.valueOf(part) : part);
            }
            return out;
        }
        if (type == String.class) return first;
        if (type == Integer.class || type == int.class) return first.isEmpty() ? null : Integer.valueOf(first);
        if (type == Long.class || type == long.class) return first.isEmpty() ? null : Long.valueOf(first);
        if (type == Double.class || type == double.class) return first.isEmpty() ? null : Double.valueOf(first);
        if (type == Boolean.class || type == boolean.class) return Boolean.valueOf(first);
        return null;
    }

    static jakarta.servlet.http.HttpServletRequest request(Map<String, List<String>> params) {
        Map<String, Object> attributes = new HashMap<>();
        return (jakarta.servlet.http.HttpServletRequest) Proxy.newProxyInstance(
                PageProfiler.class.getClassLoader(), new Class<?>[]{jakarta.servlet.http.HttpServletRequest.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getAttribute" -> attributes.get((String) args[0]);
                    case "setAttribute" -> { attributes.put((String) args[0], args[1]); yield null; }
                    case "removeAttribute" -> attributes.remove((String) args[0]);
                    case "getAttributeNames" -> Collections.enumeration(attributes.keySet());
                    case "getParameterMap" -> {
                        Map<String, String[]> m = new LinkedHashMap<>();
                        params.forEach((k, v) -> m.put(k, v.toArray(new String[0])));
                        yield m;
                    }
                    case "getParameter" -> params.containsKey((String) args[0]) ? params.get((String) args[0]).getFirst() : null;
                    case "getParameterValues" -> params.containsKey((String) args[0]) ? params.get((String) args[0]).toArray(new String[0]) : null;
                    case "getParameterNames" -> Collections.enumeration(params.keySet());
                    case "getQueryString" -> null;
                    case "getRequestURI", "getServletPath" -> "/";
                    case "getContextPath" -> "";
                    case "getMethod" -> "GET";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "ProfilerRequest";
                    default -> null;
                });
    }

    static class TimedJdbc extends JdbcTemplate {
        final List<String[]> stages = Collections.synchronizedList(new ArrayList<>());
        TimedJdbc(DataSource source) { super(source); }
        private void record(String sql, long start, List<Object> params) {
            String caller = Arrays.stream(Thread.currentThread().getStackTrace())
                    .filter(f -> f.getClassName().startsWith("library."))
                    .findFirst().map(f -> f.getClassName().replace("library.", "") + "." + f.getMethodName() + ":" + f.getLineNumber()).orElse("unknown");
            // JSON-encoded so SQL line comments survive a round trip through the TSV.
            String s = "\"" + sql.strip().replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", "")
                    .replace("\n", "\\n").replace("\t", "\\t") + "\"";
            StringBuilder p = new StringBuilder("[");
            for (int i = 0; i < params.size(); i++) {
                Object v = params.get(i);
                if (i > 0) p.append(',');
                if (v == null) p.append("null");
                else if (v instanceof Number || v instanceof Boolean) p.append(v);
                else p.append('"').append(v.toString().replace("\\", "\\\\").replace("\"", "\\\"").replaceAll("[\\t\\n\\r]", " ")).append('"');
            }
            p.append(']');
            stages.add(new String[]{String.format(Locale.ROOT, "%.4f", (System.nanoTime() - start) / 1e9), caller, s + "\t" + p});
        }
        @Override public <T> T query(String sql, PreparedStatementSetter setter, ResultSetExtractor<T> extractor) {
            long start = System.nanoTime();
            List<Object> params = new ArrayList<>();
            PreparedStatementSetter recording = setter == null ? null : ps -> setter.setValues(recorder(ps, params));
            try { return super.query(sql, recording, extractor); } finally { record(sql, start, params); }
        }
        @Override public <T> T query(String sql, ResultSetExtractor<T> extractor) {
            long start = System.nanoTime();
            try { return super.query(sql, extractor); } finally { record(sql, start, List.of()); }
        }
        static java.sql.PreparedStatement recorder(java.sql.PreparedStatement ps, List<Object> params) {
            return (java.sql.PreparedStatement) Proxy.newProxyInstance(PageProfiler.class.getClassLoader(), new Class<?>[]{java.sql.PreparedStatement.class},
                    (proxy, method, args) -> {
                        if (method.getName().startsWith("set") && args != null && args.length >= 2 && args[0] instanceof Integer idx) {
                            while (params.size() < idx) params.add(null);
                            params.set(idx - 1, method.getName().equals("setNull") ? null : args[1]);
                        }
                        try { return method.invoke(ps, args); } catch (InvocationTargetException e) { throw e.getCause(); }
                    });
        }
    }
}
