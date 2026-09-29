import library.dto.AlbumStatsQuery;
import library.repository.AlbumRepository;
import library.service.AlbumFullListenCalculator;
import library.service.AppConfigService;
import library.service.TimeframeService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;

/** Explicit, read-only benchmark. No Spring context, server, scheduler or database writes. */
public class SlowPathBenchmark {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("Usage: <SQLite file> <output prefix> [runs=2] [path substring]");
        var source = new SingleConnectionDataSource("jdbc:sqlite:" + Path.of(args[0]).toUri() + "?mode=ro", true);
        try {
            var jdbc = new JdbcTemplate(source);
            jdbc.execute("PRAGMA query_only=ON");
            jdbc.execute("PRAGMA busy_timeout=10000");
            var config = new AppConfigService(jdbc, false, "", "", 10, 20, 7, 23);
            var albums = new AlbumRepository(jdbc, new AlbumFullListenCalculator(jdbc, config));
            var timeframes = new TimeframeService(jdbc);
            var cases = new LinkedHashMap<String, Map<String, Object>>();
            cases.put("/albums?fullAlbumPlaysMin=1", Map.of("fullAlbumPlaysMin", 1));
            for (String filter : List.of("malePlayPctMin", "playsMin")) {
                cases.put("/decades?" + filter + "=" + (filter.equals("playsMin") ? "100" : "50"),
                        Map.of("periodType", "decades", filter, filter.equals("playsMin") ? (Object) 100 : 50.0));
            }
            for (String sort : List.of("plays", "maleplaypct", "random", "maledays")) {
                cases.put("/decades?sortby=" + sort + "&sortdir=desc&randomSeed=42",
                        Map.of("periodType", "decades", "sortBy", sort, "randomSeed", 42));
            }
            cases.put("/decades?maleDaysMin=1", Map.of("periodType", "decades", "maleDaysMin", 1));
            cases.put("/years?malePlayPctMin=50", Map.of("periodType", "years", "malePlayPctMin", 50.0));
            cases.put("/years?maleDaysMin=1", Map.of("periodType", "years", "maleDaysMin", 1));
            if (args.length > 3 && args[3].equals("extended")) {
                for (String period : List.of("days", "weeks", "months", "seasons", "years", "decades")) {
                    cases.put("/" + period, Map.of("periodType", period));
                    cases.put("/" + period + "?winningGender=2&winningGenderMode=includes",
                            Map.of("periodType", period, "winningGender", List.of(2), "winningGenderMode", "includes"));
                }
                for (String sort : List.of("full_album_plays", "first_full_listen", "last_full_listen")) {
                    cases.put("/albums?sortby=" + sort, Map.of("sortBy", sort));
                }
            }
            var timings = new StringBuilder("run\tseconds\tpath\n");
            int runs = args.length > 2 ? Integer.parseInt(args[2]) : 2;
            for (int run = 1; run <= runs; run++) {
                for (var entry : cases.entrySet()) {
                    if (args.length > 3 && !args[3].equals("extended") && !entry.getKey().contains(args[3])) continue;
                    var values = new HashMap<String, Object>(entry.getValue());
                    values.putIfAbsent("sortBy", entry.getKey().startsWith("/albums") ? "plays" : "period");
                    values.put("sortDir", "desc");
                    values.put("page", 0);
                    values.put("perPage", config.getTimeframesListPageSize());
                    values.put("limit", config.getAlbumsListPageSize());
                    values.put("offset", 0);
                    long start = System.nanoTime();
                    Object result;
                    if (entry.getKey().startsWith("/albums")) {
                        var components = AlbumStatsQuery.class.getRecordComponents();
                        var query = AlbumStatsQuery.class.getDeclaredConstructor(Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new))
                                .newInstance(Arrays.stream(components).map(c -> value(c.getName(), c.getType(), values)).toArray());
                        result = List.of(albums.findAlbumsWithStats(query), invoke(albums, "countAlbumsWithFilters", values), invoke(albums, "countAlbumsByGenderWithFilters", values));
                    } else {
                        result = invoke(timeframes, "getTimeframeCardsWithCount", values);
                    }
                    double seconds = (System.nanoTime() - start) / 1e9;
                    String line = String.format(Locale.ROOT, "%d\t%.3f\t%s%n", run, seconds, entry.getKey());
                    System.out.print(line);
                    timings.append(line);
                    Files.writeString(Path.of(args[1] + "-timings.tsv"), timings);
                    Files.writeString(Path.of(args[1] + "-" + cases.keySet().stream().toList().indexOf(entry.getKey()) + "-results.txt"), snapshot(result));
                }
            }
        } finally {
            source.destroy();
        }
    }

    private static Object invoke(Object target, String name, Map<String, Object> values) throws Exception {
        Method method = Arrays.stream(target.getClass().getMethods()).filter(m -> m.getName().equals(name))
                .max(Comparator.comparingInt(Method::getParameterCount)).orElseThrow();
        return method.invoke(target, Arrays.stream(method.getParameters()).map(p -> {
            if (!p.isNamePresent()) throw new IllegalStateException("Compile application with -parameters");
            return value(p.getName(), p.getType(), values);
        }).toArray());
    }

    private static Object value(String name, Class<?> type, Map<String, Object> values) {
        if (values.containsKey(name)) return values.get(name);
        return type == boolean.class ? false : type == int.class ? 0 : null;
    }

    static String snapshot(Object object) throws Exception {
        if (object == null) return "null";
        if (object instanceof Map<?, ?> map) {
            var sorted = new TreeMap<String, String>();
            for (var entry : map.entrySet()) sorted.put(entry.getKey().toString(), snapshot(entry.getValue()));
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
}
