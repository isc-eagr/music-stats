package library.service;

import jakarta.servlet.http.HttpServletRequest;
import library.dto.ChartFilterDTO;
import library.util.DateFormatUtils;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class ChartFilterRequestFactory {

    public ChartFilterDTO build(HttpServletRequest request, boolean includeGroups, boolean includeFeatured, Integer limit, String limitEntity, String catalogType) {
        String normalizedCatalogType = normalizeCatalogType(catalogType);
        return ChartFilterDTO.builder()
            .setName(request.getParameter("q"))
            .setCatalogType(normalizedCatalogType)
            .setArtistIds(parseIntegerList(request, "artist"))
            .setAlbumIds(parseIntegerList(request, "album"))
            .setSongIds(parseIntegerList(request, "song"))
            .setGenreIds(parseIntegerList(request, "genre"))
            .setGenreMode(request.getParameter("genreMode"))
            .setSubgenreIds(parseIntegerList(request, "subgenre"))
            .setSubgenreMode(request.getParameter("subgenreMode"))
            .setLanguageIds(parseIntegerList(request, "language"))
            .setLanguageMode(request.getParameter("languageMode"))
            .setGenderIds(parseIntegerList(request, "gender"))
            .setGenderMode(request.getParameter("genderMode"))
            .setEthnicityIds(parseIntegerList(request, "ethnicity"))
            .setEthnicityMode(request.getParameter("ethnicityMode"))
            .setCountries(parseStringList(request, "country"))
            .setCountryMode(request.getParameter("countryMode"))
            .setAccounts(parseStringList(request, "account"))
            .setAccountMode(request.getParameter("accountMode"))
            .setReleaseDate(toIso(request.getParameter("releaseDate")))
            .setReleaseDateFrom(toIso(request.getParameter("releaseDateFrom")))
            .setReleaseDateTo(toIso(request.getParameter("releaseDateTo")))
            .setReleaseDateMode(request.getParameter("releaseDateMode"))
            .setFirstListenedDate(toIso(request.getParameter("firstListenedDate")))
            .setFirstListenedDateFrom(toIso(request.getParameter("firstListenedDateFrom")))
            .setFirstListenedDateTo(toIso(request.getParameter("firstListenedDateTo")))
            .setFirstListenedDateMode(request.getParameter("firstListenedDateMode"))
            .setFirstListenedDateEntity(defaultEntityParam(request.getParameter("firstListenedDateEntity"), normalizedCatalogType))
            .setLastListenedDate(toIso(request.getParameter("lastListenedDate")))
            .setLastListenedDateFrom(toIso(request.getParameter("lastListenedDateFrom")))
            .setLastListenedDateTo(toIso(request.getParameter("lastListenedDateTo")))
            .setLastListenedDateMode(request.getParameter("lastListenedDateMode"))
            .setLastListenedDateEntity(defaultEntityParam(request.getParameter("lastListenedDateEntity"), normalizedCatalogType))
            .setLastFullListenDate(toIso(request.getParameter("lastFullListenDate")))
            .setLastFullListenDateFrom(toIso(request.getParameter("lastFullListenDateFrom")))
            .setLastFullListenDateTo(toIso(request.getParameter("lastFullListenDateTo")))
            .setLastFullListenDateMode(request.getParameter("lastFullListenDateMode"))
            .setListenedDateFrom(toIso(request.getParameter("listenedDateFrom")))
            .setListenedDateTo(toIso(request.getParameter("listenedDateTo")))
            .setPlayCountMin(parseInteger(request, "playCountMin"))
            .setPlayCountMax(parseInteger(request, "playCountMax"))
            .setPlayCountEntity(defaultEntityParam(request.getParameter("playCountEntity"), normalizedCatalogType))
            .setHasFeaturedArtists(request.getParameter("hasFeaturedArtists"))
            .setIsBand(request.getParameter("isBand"))
            .setIsSingle(request.getParameter("isSingle"))
            .setInItunes(request.getParameter("inItunes"))
            .setOrganized(request.getParameter("organized"))
            .setImageCountMin(parseInteger(request, "imageCountMin"))
            .setImageCountMax(parseInteger(request, "imageCountMax"))
            .setImageTheme(parseInteger(request, "imageTheme"))
            .setImageThemeMode(request.getParameter("imageThemeMode"))
            .setAlbumCountMin(parseInteger(request, "albumCountMin"))
            .setAlbumCountMax(parseInteger(request, "albumCountMax"))
            .setSongCountMin(parseInteger(request, "songCountMin"))
            .setSongCountMax(parseInteger(request, "songCountMax"))
            .setTrackNumber(parseInteger(request, "trackNumber"))
            .setTrackNumberMode(request.getParameter("trackNumberMode"))
            .setLengthMin(parseInteger(request, "lengthMin"))
            .setLengthMax(parseInteger(request, "lengthMax"))
            .setLengthMode(request.getParameter("lengthMode"))
            .setItunesPresenceMin(parseInteger(request, "itunesPresenceMin"))
            .setItunesPresenceMax(parseInteger(request, "itunesPresenceMax"))
            .setAgeMin(parseInteger(request, "ageMin"))
            .setAgeMax(parseInteger(request, "ageMax"))
            .setAgeMode(request.getParameter("ageMode"))
            .setAgeAtReleaseMin(parseInteger(request, "ageAtReleaseMin"))
            .setAgeAtReleaseMax(parseInteger(request, "ageAtReleaseMax"))
            .setBirthDate(toIso(request.getParameter("birthDate")))
            .setBirthDateFrom(toIso(request.getParameter("birthDateFrom")))
            .setBirthDateTo(toIso(request.getParameter("birthDateTo")))
            .setBirthDateMode(request.getParameter("birthDateMode"))
            .setDeathDate(toIso(request.getParameter("deathDate")))
            .setDeathDateFrom(toIso(request.getParameter("deathDateFrom")))
            .setDeathDateTo(toIso(request.getParameter("deathDateTo")))
            .setDeathDateMode(request.getParameter("deathDateMode"))
            .setAlbumsWeeklyChartPeak(resolveChartMetric(request, normalizedCatalogType, "album", "albumsWeeklyChartPeak", "weeklyChartPeak"))
            .setAlbumsWeeklyChartWeeks(resolveChartMetric(request, normalizedCatalogType, "album", "albumsWeeklyChartWeeks", "weeklyChartWeeks"))
            .setAlbumsSeasonalChartPeak(resolveChartMetric(request, normalizedCatalogType, "album", "albumsSeasonalChartPeak", "seasonalChartPeak"))
            .setAlbumsSeasonalChartSeasons(resolveChartMetric(request, normalizedCatalogType, "album", "albumsSeasonalChartSeasons", "seasonalChartSeasons"))
            .setAlbumsYearlyChartPeak(resolveChartMetric(request, normalizedCatalogType, "album", "albumsYearlyChartPeak", "yearlyChartPeak"))
            .setAlbumsYearlyChartYears(resolveChartMetric(request, normalizedCatalogType, "album", "albumsYearlyChartYears", "yearlyChartYears"))
            .setSongsWeeklyChartPeak(resolveChartMetric(request, normalizedCatalogType, "song", "songsWeeklyChartPeak", "weeklyChartPeak"))
            .setSongsWeeklyChartWeeks(resolveChartMetric(request, normalizedCatalogType, "song", "songsWeeklyChartWeeks", "weeklyChartWeeks"))
            .setSongsTrlPeak(resolveChartMetric(request, normalizedCatalogType, "song", "songsTrlPeak", "trlPeak"))
            .setSongsTrlDays(resolveChartMetric(request, normalizedCatalogType, "song", "songsTrlDays", "trlDays"))
            .setSongsVatosCuntdownPeak(resolveChartMetric(request, normalizedCatalogType, "song", "songsVatosCuntdownPeak", "vatosCuntdownPeak"))
            .setSongsVatosCuntdownDays(resolveChartMetric(request, normalizedCatalogType, "song", "songsVatosCuntdownDays", "vatosCuntdownDays"))
            .setSongsBillboardPeak(resolveChartMetric(request, normalizedCatalogType, "song", "songsBillboardPeak", "billboardPeak"))
            .setSongsBillboardWeeks(resolveChartMetric(request, normalizedCatalogType, "song", "songsBillboardWeeks", "billboardWeeks"))
            .setSongsSeasonalChartPeak(resolveChartMetric(request, normalizedCatalogType, "song", "songsSeasonalChartPeak", "seasonalChartPeak"))
            .setSongsSeasonalChartSeasons(resolveChartMetric(request, normalizedCatalogType, "song", "songsSeasonalChartSeasons", "seasonalChartSeasons"))
            .setSongsYearlyChartPeak(resolveChartMetric(request, normalizedCatalogType, "song", "songsYearlyChartPeak", "yearlyChartPeak"))
            .setSongsYearlyChartYears(resolveChartMetric(request, normalizedCatalogType, "song", "songsYearlyChartYears", "yearlyChartYears"))
            .setTopLimit(limit)
            .setLimitEntity(limitEntity)
            .setIncludeGroups(includeGroups)
            .setIncludeFeatured(includeFeatured);
    }

    private String normalizeCatalogType(String catalogType) {
        if (catalogType == null || catalogType.isBlank()) {
            return "song";
        }
        return switch (catalogType.trim().toLowerCase()) {
            case "artists", "artist" -> "artist";
            case "albums", "album" -> "album";
            default -> "song";
        };
    }

    private String defaultEntityParam(String explicitValue, String catalogType) {
        if (explicitValue != null && !explicitValue.isBlank()) {
            return explicitValue;
        }
        return catalogType;
    }

    private Integer resolveChartMetric(HttpServletRequest request, String catalogType, String targetCatalogType, String explicitParam, String fallbackParam) {
        Integer explicitValue = parseInteger(request, explicitParam);
        if (explicitValue != null) {
            return explicitValue;
        }
        if (catalogType.equals(targetCatalogType)) {
            return parseInteger(request, fallbackParam);
        }
        return null;
    }

    private String toIso(String value) {
        return DateFormatUtils.convertToIsoFormat(value);
    }

    private Integer parseInteger(HttpServletRequest request, String paramName) {
        String value = request.getParameter(paramName);
        if (value == null || value.isBlank()) {
            return null;
        }

        try {
            return Integer.valueOf(value.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private List<Integer> parseIntegerList(HttpServletRequest request, String paramName) {
        String[] values = request.getParameterValues(paramName);
        if (values == null || values.length == 0) {
            return null;
        }

        return Arrays.stream(values)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .map(value -> {
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException ex) {
                    return null;
                }
            })
            .filter(value -> value != null)
            .toList();
    }

    private List<String> parseStringList(HttpServletRequest request, String paramName) {
        String[] values = request.getParameterValues(paramName);
        if (values == null || values.length == 0) {
            return null;
        }

        return Arrays.stream(values)
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .toList();
    }
}
