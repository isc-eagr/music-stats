package library.service;

import jakarta.servlet.http.HttpServletRequest;
import library.dto.ChartFilterDTO;
import library.util.DateFormatUtils;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class ChartFilterRequestFactory {

    public ChartFilterDTO build(HttpServletRequest request, boolean includeGroups, boolean includeFeatured, Integer limit, String limitEntity) {
        return ChartFilterDTO.builder()
            .setName(request.getParameter("q"))
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
            .setFirstListenedDateEntity(request.getParameter("firstListenedDateEntity"))
            .setLastListenedDate(toIso(request.getParameter("lastListenedDate")))
            .setLastListenedDateFrom(toIso(request.getParameter("lastListenedDateFrom")))
            .setLastListenedDateTo(toIso(request.getParameter("lastListenedDateTo")))
            .setLastListenedDateMode(request.getParameter("lastListenedDateMode"))
            .setLastListenedDateEntity(request.getParameter("lastListenedDateEntity"))
            .setLastFullListenDate(toIso(request.getParameter("lastFullListenDate")))
            .setLastFullListenDateFrom(toIso(request.getParameter("lastFullListenDateFrom")))
            .setLastFullListenDateTo(toIso(request.getParameter("lastFullListenDateTo")))
            .setLastFullListenDateMode(request.getParameter("lastFullListenDateMode"))
            .setListenedDateFrom(toIso(request.getParameter("listenedDateFrom")))
            .setListenedDateTo(toIso(request.getParameter("listenedDateTo")))
            .setPlayCountMin(parseInteger(request, "playCountMin"))
            .setPlayCountMax(parseInteger(request, "playCountMax"))
            .setPlayCountEntity(request.getParameter("playCountEntity"))
            .setHasFeaturedArtists(request.getParameter("hasFeaturedArtists"))
            .setIsBand(request.getParameter("isBand"))
            .setIsSingle(request.getParameter("isSingle"))
            .setInItunes(request.getParameter("inItunes"))
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
            .setAlbumsWeeklyChartPeak(parseInteger(request, "albumsWeeklyChartPeak"))
            .setAlbumsWeeklyChartWeeks(parseInteger(request, "albumsWeeklyChartWeeks"))
            .setAlbumsSeasonalChartPeak(parseInteger(request, "albumsSeasonalChartPeak"))
            .setAlbumsSeasonalChartSeasons(parseInteger(request, "albumsSeasonalChartSeasons"))
            .setAlbumsYearlyChartPeak(parseInteger(request, "albumsYearlyChartPeak"))
            .setAlbumsYearlyChartYears(parseInteger(request, "albumsYearlyChartYears"))
            .setSongsWeeklyChartPeak(parseInteger(request, "songsWeeklyChartPeak"))
            .setSongsWeeklyChartWeeks(parseInteger(request, "songsWeeklyChartWeeks"))
            .setSongsTrlPeak(parseInteger(request, "songsTrlPeak"))
            .setSongsTrlDays(parseInteger(request, "songsTrlDays"))
            .setSongsVatosCuntdownPeak(parseInteger(request, "songsVatosCuntdownPeak"))
            .setSongsVatosCuntdownDays(parseInteger(request, "songsVatosCuntdownDays"))
            .setSongsBillboardPeak(parseInteger(request, "songsBillboardPeak"))
            .setSongsBillboardWeeks(parseInteger(request, "songsBillboardWeeks"))
            .setSongsSeasonalChartPeak(parseInteger(request, "songsSeasonalChartPeak"))
            .setSongsSeasonalChartSeasons(parseInteger(request, "songsSeasonalChartSeasons"))
            .setSongsYearlyChartPeak(parseInteger(request, "songsYearlyChartPeak"))
            .setSongsYearlyChartYears(parseInteger(request, "songsYearlyChartYears"))
            .setTopLimit(limit)
            .setLimitEntity(limitEntity)
            .setIncludeGroups(includeGroups)
            .setIncludeFeatured(includeFeatured);
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
