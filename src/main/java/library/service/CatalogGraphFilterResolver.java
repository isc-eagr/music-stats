package library.service;

import jakarta.servlet.http.HttpServletRequest;
import library.dto.AlbumStatsQuery;
import library.dto.AlbumStatsRow;
import library.dto.ArtistStatsQuery;
import library.dto.ArtistStatsRow;
import library.dto.ChartFilterDTO;
import library.dto.SongStatsQuery;
import library.dto.SongStatsRow;
import library.repository.AlbumRepository;
import library.repository.ArtistRepositoryImpl;
import library.repository.SongRepository;
import library.util.ArtistFilterMode;
import library.util.DateFormatUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Resolves graph scope through the same complete queries used by catalog lists. */
@Component
public class CatalogGraphFilterResolver {
    private static final Map<String, String> REQUEST_NAMES = Map.ofEntries(
            Map.entry("name", "q"), Map.entry("artistName", "artist"),
            Map.entry("albumName", "album"), Map.entry("featuredArtistIds", "featuredArtist"),
            Map.entry("genderIds", "gender"), Map.entry("ethnicityIds", "ethnicity"),
            Map.entry("genreIds", "genre"), Map.entry("subgenreIds", "subgenre"),
            Map.entry("languageIds", "language"), Map.entry("countries", "country"),
            Map.entry("tagIds", "tag"), Map.entry("accounts", "account"));
    private static final Set<String> INTERNAL_FIELDS = Set.of(
            "limit", "offset", "includeExtendedStats", "includeExpensiveStats", "includeFullListenStats",
            "itunesIdsJson", "itunesSongIdsJson", "songIdsFilter", "randomSeed",
            "sortBy", "sortBy2", "sortBy3", "sortDir", "sortDir2", "sortDir3",
            "sortDirection", "sortDirection2", "sortDirection3", "includeMain", "includeGroups", "includeFeatured");

    private final ArtistRepositoryImpl artistRepository;
    private final AlbumRepository albumRepository;
    private final SongRepository songRepository;
    private final ItunesService itunesService;

    public CatalogGraphFilterResolver(ArtistRepositoryImpl artistRepository, AlbumRepository albumRepository,
                                      SongRepository songRepository, ItunesService itunesService) {
        this.artistRepository = artistRepository;
        this.albumRepository = albumRepository;
        this.songRepository = songRepository;
        this.itunesService = itunesService;
    }

    public ChartFilterDTO resolve(HttpServletRequest request, String source) {
        Class<? extends Record> queryType = switch (source) {
            case "artists" -> ArtistStatsQuery.class;
            case "albums" -> AlbumStatsQuery.class;
            case "songs" -> SongStatsQuery.class;
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown graph source");
        };
        ChartFilterDTO filter = ChartFilterDTO.builder()
                .setCatalogType(source)
                .setIncludeMain(booleanParameter(request, "includeMain", true))
                .setIncludeGroups(booleanParameter(request, "includeGroups", false))
                .setIncludeFeatured(booleanParameter(request, "includeFeatured", false))
                .setListenedDateFrom(DateFormatUtils.convertToIsoFormat(request.getParameter("listenedDateFrom")))
                .setListenedDateTo(DateFormatUtils.convertToIsoFormat(request.getParameter("listenedDateTo")))
                .setTopLimit(integerParameter(request.getParameter("limit"), "limit"))
                .setLimitEntity(request.getParameter("limitEntity"));
        String accountMode = request.getParameter("accountMode");
        if ("includes".equalsIgnoreCase(accountMode) || "excludes".equalsIgnoreCase(accountMode)) {
            filter.setAccounts(stringList(request, "account")).setAccountMode(accountMode);
        }

        // Keep the direct-table fast path for the initial, unfiltered explorer.
        if (!hasFilters(request, queryType)) {
            return filter;
        }
        List<Integer> ids = switch (source) {
            case "artists" -> artistRepository.findArtistsWithStats(bind(request, ArtistStatsQuery.class, source))
                    .stream().map(ArtistStatsRow::id).toList();
            case "albums" -> albumRepository.findAlbumsWithStats(bind(request, AlbumStatsQuery.class, source))
                    .stream().map(AlbumStatsRow::id).toList();
            default -> songRepository.findSongsWithStats(bind(request, SongStatsQuery.class, source))
                    .stream().map(SongStatsRow::id).toList();
        };
        return filter.setMatchedCatalogIdsJson(ids.stream().distinct().map(String::valueOf)
                .collect(Collectors.joining(",", "[", "]")));
    }

    private boolean hasFilters(HttpServletRequest request, Class<? extends Record> queryType) {
        if (queryType == ArtistStatsQuery.class && !booleanParameter(request, "includeMain", true)) {
            return true;
        }
        return Arrays.stream(queryType.getRecordComponents())
                .filter(component -> !INTERNAL_FIELDS.contains(component.getName()))
                .anyMatch(component -> {
                    String[] values = request.getParameterValues(requestName(component.getName()));
                    return values != null && Arrays.stream(values).anyMatch(value -> value != null && !value.isBlank());
                });
    }

    private <T extends Record> T bind(HttpServletRequest request, Class<T> queryType, String source) {
        RecordComponent[] components = queryType.getRecordComponents();
        Class<?>[] types = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);
        Object[] values = Arrays.stream(components).map(component -> value(request, component, source)).toArray();
        try {
            return queryType.getDeclaredConstructor(types).newInstance(values);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Unable to construct catalog filter query", exception);
        }
    }

    private Object value(HttpServletRequest request, RecordComponent component, String source) {
        String field = component.getName();
        return switch (field) {
            // Pagination and display-only stats never restrict a graph's matching set.
            case "limit" -> -1;
            case "offset" -> 0;
            case "sortBy" -> "name";
            case "includeExtendedStats", "includeExpensiveStats", "includeFullListenStats" -> false;
            case "sortBy2", "sortBy3", "sortDir", "sortDir2", "sortDir3", "sortDirection",
                 "sortDirection2", "sortDirection3", "randomSeed", "songIdsFilter" -> null;
            case "itunesIdsJson" -> itunesIds(request, source);
            case "itunesSongIdsJson" -> request.getParameter("itunesPresenceMin") != null
                    || request.getParameter("itunesPresenceMax") != null ? itunesService.getAllItunesSongIdsJson() : null;
            default -> requestValue(request, component);
        };
    }

    private Object requestValue(HttpServletRequest request, RecordComponent component) {
        String field = component.getName();
        String parameter = requestName(field);
        if (component.getType() == List.class) {
            List<String> entries = stringList(request, parameter);
            if (entries == null) return null;
            Class<?> elementType = (Class<?>) ((ParameterizedType) component.getGenericType()).getActualTypeArguments()[0];
            if (elementType == String.class) return entries;
            List<Integer> ids = entries.stream().map(value -> integerParameter(value, parameter)).toList();
            return "artistName".equals(field) ? ArtistFilterMode.encode(ids, request.getParameter("artistMode")) : ids;
        }
        if (component.getType() == boolean.class) {
            return booleanParameter(request, parameter, "includeMain".equals(field));
        }
        String text = request.getParameter(parameter);
        if (component.getType() == Integer.class) return integerParameter(text, parameter);
        if (field.contains("Date") && !field.endsWith("Mode")) return DateFormatUtils.convertToIsoFormat(text);
        return text;
    }

    private String itunesIds(HttpServletRequest request, String source) {
        String inItunes = request.getParameter("inItunes");
        if (inItunes == null || inItunes.isBlank()) return null;
        return switch (source) {
            case "artists" -> itunesService.getAllItunesArtistIdsJson();
            case "albums" -> itunesService.getAllItunesAlbumIdsJson();
            default -> itunesService.getAllItunesSongIdsJson();
        };
    }

    private String requestName(String field) {
        return REQUEST_NAMES.getOrDefault(field, field);
    }

    private List<String> stringList(HttpServletRequest request, String parameter) {
        String[] values = request.getParameterValues(parameter);
        if (values == null) return null;
        List<String> entries = Arrays.stream(values).flatMap(value -> Arrays.stream(value.split(",")))
                .map(String::trim).filter(value -> !value.isEmpty()).toList();
        return entries.isEmpty() ? null : entries;
    }

    private Integer integerParameter(String value, String parameter) {
        if (value == null || value.isBlank()) return null;
        try {
            return Integer.valueOf(value.trim());
        } catch (NumberFormatException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid integer filter: " + parameter);
        }
    }

    private boolean booleanParameter(HttpServletRequest request, String parameter, boolean defaultValue) {
        String value = request.getParameter(parameter);
        if (value == null || value.isBlank()) return defaultValue;
        return switch (value.toLowerCase(java.util.Locale.ROOT)) {
            case "true", "on", "yes", "1" -> true;
            case "false", "off", "no", "0" -> false;
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid boolean filter: " + parameter);
        };
    }
}
