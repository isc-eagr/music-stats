package library.service;

import library.dto.ChartFilterDTO;
import library.dto.FeaturedArtistCardDTO;
import library.dto.FeaturedArtistDTO;
import library.dto.GenderCountDTO;
import library.dto.PlaysByYearDTO;
import library.dto.PlaysByMonthDTO;
import library.dto.PlayDTO;
import library.dto.SongCardDTO;
import library.entity.Song;
import library.entity.SongImage;
import library.repository.LookupRepository;
import library.repository.SongImageRepository;
import library.repository.SongRepository;
import library.util.TimeFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SongService {

    private static final String COMBINED_SONG_COUNT_REQUEST_CACHE = SongService.class.getName() + ".combinedSongCountRequestCache";
    private static final DateTimeFormatter DISPLAY_DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter LEGACY_DISPLAY_DATE_FORMATTER = DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH);
    
    private final SongRepository songRepository;
    private final SongImageRepository songImageRepository;
    private final LookupRepository lookupRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ItunesService itunesService;
    private final AppConfigService appConfigService;
    private final SongLinkService songLinkService;
    
    public SongService(SongRepository songRepository, SongImageRepository songImageRepository, LookupRepository lookupRepository, JdbcTemplate jdbcTemplate,
                       ItunesService itunesService, AppConfigService appConfigService, SongLinkService songLinkService) {
        this.songRepository = songRepository;
        this.songImageRepository = songImageRepository;
        this.lookupRepository = lookupRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.itunesService = itunesService;
        this.appConfigService = appConfigService;
        this.songLinkService = songLinkService;
    }

    public String getItunesSongIdsJson(String inItunes) {
        if (inItunes == null || inItunes.isEmpty()) return null;
        return itunesService.getAllItunesSongIdsJson();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Long> getCombinedSongCountRequestCache() {
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        if (requestAttributes == null) {
            return null;
        }

        Object existing = requestAttributes.getAttribute(COMBINED_SONG_COUNT_REQUEST_CACHE, RequestAttributes.SCOPE_REQUEST);
        if (existing instanceof Map<?, ?> cache) {
            return (Map<String, Long>) cache;
        }

        Map<String, Long> cache = new HashMap<>();
        requestAttributes.setAttribute(COMBINED_SONG_COUNT_REQUEST_CACHE, cache, RequestAttributes.SCOPE_REQUEST);
        return cache;
    }

    private Long getCachedCombinedSongCount(String cacheKey) {
        Map<String, Long> cache = getCombinedSongCountRequestCache();
        return cache != null ? cache.get(cacheKey) : null;
    }

    private void cacheCombinedSongCount(String cacheKey, long count) {
        Map<String, Long> cache = getCombinedSongCountRequestCache();
        if (cache != null) {
            cache.put(cacheKey, count);
        }
    }

    private <T> List<T> snapshotList(List<T> values) {
        return values == null ? null : List.copyOf(values);
    }

    private String buildCombinedSongsFilterCacheKey(String name, List<Integer> artistName, String albumName,
                                                    List<Integer> genreIds, String genreMode,
                                                    List<Integer> subgenreIds, String subgenreMode,
                                                    List<Integer> languageIds, String languageMode,
                                                    List<Integer> genderIds, String genderMode,
                                                    List<Integer> ethnicityIds, String ethnicityMode,
                                                    List<String> countries, String countryMode,
                                                    List<Integer> tagIds, String tagMode,
                                                    List<String> accounts, String accountMode,
                                                    String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                                    String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                                    String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                                    String listenedDateFrom, String listenedDateTo,
                                                    String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                                                    Integer ageMin, Integer ageMax, String ageMode,
                                                    Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                                    String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                                    String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                                    String itunesIdsJson, String inItunes,
                                                    Integer playCountMin, Integer playCountMax,
                                                    Integer trackNumber, String trackNumberMode,
                                                    Integer lengthMin, Integer lengthMax, String lengthMode,
                                                    Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                                    String weeklyChartDateFrom, String weeklyChartDateTo, String weeklyChartSeason,
                                                    Integer trlPeak, Integer trlDays,
                                                    String trlDateFrom, String trlDateTo,
                                                    Integer vatosCuntdownPeak, Integer vatosCuntdownDays,
                                                    String vatosCuntdownDateFrom, String vatosCuntdownDateTo,
                                                    Integer billboardPeak, Integer billboardWeeks,
                                                    String billboardDateFrom, String billboardDateTo,
                                                    Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                                    String seasonalChartDateFrom, String seasonalChartDateTo, String seasonalChartSeason,
                                                    Integer yearlyChartPeak, Integer yearlyChartYears,
                                                    String yearlyChartDateFrom, String yearlyChartDateTo) {
        List<Object> parts = new ArrayList<>();
        parts.add(name);
        parts.add(snapshotList(artistName));
        parts.add(albumName);
        parts.add(snapshotList(genreIds));
        parts.add(genreMode);
        parts.add(snapshotList(subgenreIds));
        parts.add(subgenreMode);
        parts.add(snapshotList(languageIds));
        parts.add(languageMode);
        parts.add(snapshotList(genderIds));
        parts.add(genderMode);
        parts.add(snapshotList(ethnicityIds));
        parts.add(ethnicityMode);
        parts.add(snapshotList(countries));
        parts.add(countryMode);
        parts.add(snapshotList(tagIds));
        parts.add(tagMode);
        parts.add(snapshotList(accounts));
        parts.add(accountMode);
        parts.add(releaseDate);
        parts.add(releaseDateFrom);
        parts.add(releaseDateTo);
        parts.add(releaseDateMode);
        parts.add(firstListenedDate);
        parts.add(firstListenedDateFrom);
        parts.add(firstListenedDateTo);
        parts.add(firstListenedDateMode);
        parts.add(lastListenedDate);
        parts.add(lastListenedDateFrom);
        parts.add(lastListenedDateTo);
        parts.add(lastListenedDateMode);
        parts.add(listenedDateFrom);
        parts.add(listenedDateTo);
        parts.add(organized);
        parts.add(imageCountMin);
        parts.add(imageCountMax);
        parts.add(hasFeaturedArtists);
        parts.add(isBand);
        parts.add(isSingle);
        parts.add(ageMin);
        parts.add(ageMax);
        parts.add(ageMode);
        parts.add(ageAtReleaseMin);
        parts.add(ageAtReleaseMax);
        parts.add(birthDate);
        parts.add(birthDateFrom);
        parts.add(birthDateTo);
        parts.add(birthDateMode);
        parts.add(deathDate);
        parts.add(deathDateFrom);
        parts.add(deathDateTo);
        parts.add(deathDateMode);
        parts.add(itunesIdsJson);
        parts.add(inItunes);
        parts.add(playCountMin);
        parts.add(playCountMax);
        parts.add(trackNumber);
        parts.add(trackNumberMode);
        parts.add(lengthMin);
        parts.add(lengthMax);
        parts.add(lengthMode);
        parts.add(weeklyChartPeak);
        parts.add(weeklyChartWeeks);
        parts.add(weeklyChartDateFrom);
        parts.add(weeklyChartDateTo);
        parts.add(weeklyChartSeason);
        parts.add(trlPeak);
        parts.add(trlDays);
        parts.add(trlDateFrom);
        parts.add(trlDateTo);
        parts.add(vatosCuntdownPeak);
        parts.add(vatosCuntdownDays);
        parts.add(vatosCuntdownDateFrom);
        parts.add(vatosCuntdownDateTo);
        parts.add(billboardPeak);
        parts.add(billboardWeeks);
        parts.add(billboardDateFrom);
        parts.add(billboardDateTo);
        parts.add(seasonalChartPeak);
        parts.add(seasonalChartSeasons);
        parts.add(seasonalChartDateFrom);
        parts.add(seasonalChartDateTo);
        parts.add(seasonalChartSeason);
        parts.add(yearlyChartPeak);
        parts.add(yearlyChartYears);
        parts.add(yearlyChartDateFrom);
        parts.add(yearlyChartDateTo);
        return parts.toString();
    }

    private void populateSongItunesPresence(List<SongCardDTO> songs) {
        if (songs == null || songs.isEmpty()) {
            return;
        }

        for (SongCardDTO song : songs) {
            song.setInItunes(itunesService.songExistsInItunes(song.getArtistName(), song.getAlbumName(), song.getName()));
        }
    }
    
    public List<SongCardDTO> getSongs(String name, List<Integer> artistName, String albumName,
                                       List<Integer> genreIds, String genreMode,
                                       List<Integer> subgenreIds, String subgenreMode,
                                       List<Integer> languageIds, String languageMode,
                                       List<Integer> genderIds, String genderMode,
                                       List<Integer> ethnicityIds, String ethnicityMode,
                                       List<String> countries, String countryMode,
                                       List<Integer> tagIds, String tagMode,
                                       List<String> accounts, String accountMode,
                                       String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                                       String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                                       String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                                       String listenedDateFrom, String listenedDateTo,
                                       String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                                       Integer ageMin, Integer ageMax, String ageMode,
                                       Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                                       String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                                       String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                                       String itunesIdsJson, String inItunes,
                                       Integer playCountMin, Integer playCountMax,
                                       Integer trackNumber, String trackNumberMode,
                                       Integer lengthMin, Integer lengthMax, String lengthMode,
                                       Integer weeklyChartPeak, Integer weeklyChartWeeks,
                                       String weeklyChartDateFrom, String weeklyChartDateTo, String weeklyChartSeason,
                                       Integer trlPeak, Integer trlDays,
                                       String trlDateFrom, String trlDateTo,
                                       Integer vatosCuntdownPeak, Integer vatosCuntdownDays,
                                       String vatosCuntdownDateFrom, String vatosCuntdownDateTo,
                                       Integer billboardPeak, Integer billboardWeeks,
                                       String billboardDateFrom, String billboardDateTo,
                                       Integer seasonalChartPeak, Integer seasonalChartSeasons,
                                       String seasonalChartDateFrom, String seasonalChartDateTo, String seasonalChartSeason,
                                       Integer yearlyChartPeak, Integer yearlyChartYears,
                                       String yearlyChartDateFrom, String yearlyChartDateTo,
                                       String sortBy, String sortDirection,
                                       String sortBy2, String sortDirection2,
                                       String sortBy3, String sortDirection3,
                                       int page, int perPage) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (accounts != null && accounts.isEmpty()) accounts = null;
        if (tagIds != null && tagIds.isEmpty()) tagIds = null;
        
        boolean combineLinkedSongs = appConfigService.isCombineLinkedSongsEnabled();
        int queryLimit = combineLinkedSongs ? 100000 : perPage;
        int queryOffset = combineLinkedSongs ? 0 : page * perPage;
        String combinedSongsCacheKey = combineLinkedSongs
            ? buildCombinedSongsFilterCacheKey(
                name, artistName, albumName,
                genreIds, genreMode,
                subgenreIds, subgenreMode,
                languageIds, languageMode,
                genderIds, genderMode,
                ethnicityIds, ethnicityMode,
                countries, countryMode,
                tagIds, tagMode,
                accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks,
                weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                trlPeak, trlDays,
                trlDateFrom, trlDateTo,
                vatosCuntdownPeak, vatosCuntdownDays,
                vatosCuntdownDateFrom, vatosCuntdownDateTo,
                billboardPeak, billboardWeeks,
                billboardDateFrom, billboardDateTo,
                seasonalChartPeak, seasonalChartSeasons,
                seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears,
                yearlyChartDateFrom, yearlyChartDateTo
            )
            : null;

        List<Object[]> results = songRepository.findSongsWithStats(
                name, artistName, albumName, genreIds, genreMode, 
                subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                ethnicityIds, ethnicityMode, countries, countryMode, tagIds, tagMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                itunesIdsJson, inItunes,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks,
                weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                trlPeak, trlDays,
                trlDateFrom, trlDateTo,
                vatosCuntdownPeak, vatosCuntdownDays,
                vatosCuntdownDateFrom, vatosCuntdownDateTo,
                billboardPeak, billboardWeeks,
                billboardDateFrom, billboardDateTo,
                seasonalChartPeak, seasonalChartSeasons,
                seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears,
                yearlyChartDateFrom, yearlyChartDateTo,
                sortBy, sortDirection, sortBy2, sortDirection2, sortBy3, sortDirection3, queryLimit, queryOffset
        );
        
        List<SongCardDTO> songs = new ArrayList<>();
        for (Object[] row : results) {
            SongCardDTO dto = new SongCardDTO();
            dto.setId(((Number) row[0]).intValue());
            dto.setName((String) row[1]);
            dto.setArtistName((String) row[2]);
            dto.setArtistId(((Number) row[3]).intValue());
            dto.setAlbumName((String) row[4]);
            dto.setAlbumId(row[5] != null ? ((Number) row[5]).intValue() : null);
            dto.setGenreId(row[6] != null ? ((Number) row[6]).intValue() : null);
            dto.setGenreName((String) row[7]);
            dto.setSubgenreId(row[8] != null ? ((Number) row[8]).intValue() : null);
            dto.setSubgenreName((String) row[9]);
            dto.setLanguageId(row[10] != null ? ((Number) row[10]).intValue() : null);
            dto.setLanguageName((String) row[11]);
            dto.setEthnicityId(row[12] != null ? ((Number) row[12]).intValue() : null);
            dto.setEthnicityName((String) row[13]);
            dto.setReleaseYear((String) row[14]);
            dto.setReleaseDate(row[15] != null ? formatDate((String) row[15]) : null);
            dto.setLengthSeconds(row[16] != null ? ((Number) row[16]).intValue() : null);
            dto.setHasImage(row[17] != null && ((Number) row[17]).intValue() == 1);
            dto.setGenderName((String) row[18]);
            dto.setGenderId(row[19] != null ? ((Number) row[19]).intValue() : null);
            dto.setPlayCount(row[20] != null ? ((Number) row[20]).intValue() : 0);
            dto.setVatitoPlayCount(row[21] != null ? ((Number) row[21]).intValue() : 0);
            dto.setRobertloverPlayCount(row[22] != null ? ((Number) row[22]).intValue() : 0);
            
            // Set time listened and format it
            long timeListened = row[23] != null ? ((Number) row[23]).longValue() : 0L;
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            
            // Set first and last listened dates (indices 24 and 25)
            dto.setFirstListenedDate(row[24] != null ? formatDate((String) row[24]) : null);
            dto.setLastListenedDate(row[25] != null ? formatDate((String) row[25]) : null);
            dto.setDaysListened(row[26] != null ? ((Number) row[26]).intValue() : 0);
            dto.setWeeksListened(row[27] != null ? ((Number) row[27]).intValue() : 0);
            dto.setMonthsListened(row[28] != null ? ((Number) row[28]).intValue() : 0);
            dto.setYearsListened(row[29] != null ? ((Number) row[29]).intValue() : 0);
            dto.setTrackNumber(row[56] != null ? ((Number) row[56]).intValue() : null);
            
            // Set country (inherited from artist, index 30)
            dto.setCountry((String) row[30]);
            
            // Set organized (index 31)
            dto.setOrganized(row[31] != null && ((Number) row[31]).intValue() == 1);
            
            // Set album has image (index 32)
            dto.setAlbumHasImage(row[32] != null && ((Number) row[32]).intValue() == 1);
            
            // Set isSingle (index 33)
            dto.setIsSingle(row[33] != null && ((Number) row[33]).intValue() == 1);
            
            // Set birth date (index 34) and death date (index 35)
            dto.setBirthDate(row[34] != null ? formatDate((String) row[34]) : null);
            dto.setDeathDate(row[35] != null ? formatDate((String) row[35]) : null);
            
            // Set image count (index 36)
            dto.setImageCount(row[36] != null ? ((Number) row[36]).intValue() : 0);
            
            // Set chart stats (indices 37-50)
            dto.setBillboardPeak(row[37] != null ? ((Number) row[37]).intValue() : null);
            dto.setBillboardWeeks(row[38] != null ? ((Number) row[38]).intValue() : null);
            dto.setSeasonalChartPeak(row[39] != null ? ((Number) row[39]).intValue() : null);
            dto.setTrlDays(row[40] != null ? ((Number) row[40]).intValue() : null);
            dto.setTrlPeak(row[41] != null ? ((Number) row[41]).intValue() : null);
            dto.setVatosCuntdownDays(row[42] != null ? ((Number) row[42]).intValue() : null);
            dto.setVatosCuntdownPeak(row[43] != null ? ((Number) row[43]).intValue() : null);
            dto.setWeeklyChartPeak(row[44] != null ? ((Number) row[44]).intValue() : null);
            dto.setWeeklyChartWeeks(row[45] != null ? ((Number) row[45]).intValue() : 0);
            dto.setYearlyChartPeak(row[46] != null ? ((Number) row[46]).intValue() : null);
            dto.setWeeklyChartPeakStartDate(row[47] != null ? formatDate((String) row[47]) : null);
            dto.setSeasonalChartPeakPeriod((String) row[48]);
            dto.setYearlyChartPeakPeriod((String) row[49]);
            
            // Set featured artist count (index 51) and age at release (index 52)
            dto.setFeaturedArtistCount(row[51] != null ? ((Number) row[51]).intValue() : 0);
            dto.setAgeAtRelease(row[52] != null ? ((Number) row[52]).intValue() : null);
            
            // Set peak durations (indices 53-55)
            dto.setWeeklyChartPeakWeeks(row[53] != null ? ((Number) row[53]).intValue() : null);
            dto.setSeasonalChartPeakSeasons(row[54] != null ? ((Number) row[54]).intValue() : null);
            dto.setYearlyChartPeakYears(row[55] != null ? ((Number) row[55]).intValue() : null);

            // Format length
            if (dto.getLengthSeconds() != null) {
                int minutes = dto.getLengthSeconds() / 60;
                int seconds = dto.getLengthSeconds() % 60;
                dto.setLengthFormatted(String.format("%d:%02d", minutes, seconds));
            }
            
            songs.add(dto);
        }
        
        if (combineLinkedSongs) {
            songs = combineLinkedSongCards(songs, sortBy, sortDirection, sortBy2, sortDirection2, sortBy3, sortDirection3);
            cacheCombinedSongCount(combinedSongsCacheKey, songs.size());
            int fromIndex = Math.min(page * perPage, songs.size());
            int toIndex = Math.min(fromIndex + perPage, songs.size());
            List<SongCardDTO> pagedSongs = new ArrayList<>(songs.subList(fromIndex, toIndex));
            populateSongItunesPresence(pagedSongs);
            return pagedSongs;
        }

        populateSongItunesPresence(songs);
        return songs;
    }

    private long countCombinedRows(List<Object[]> rows) {
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        List<Integer> songIds = rows.stream()
                .map(row -> ((Number) row[0]).intValue())
                .toList();
        Map<Integer, Integer> groupIds = songLinkService.getGroupIdsForSongs(songIds);
        return rows.stream()
                .map(row -> {
                    Integer songId = ((Number) row[0]).intValue();
                    Integer groupId = groupIds.get(songId);
                    return groupId != null ? "g:" + groupId : "s:" + songId;
                })
                .distinct()
                .count();
    }

    private List<SongCardDTO> combineLinkedSongCards(List<SongCardDTO> songs,
                                                     String sortBy, String sortDirection,
                                                     String sortBy2, String sortDirection2,
                                                     String sortBy3, String sortDirection3) {
        if (songs == null || songs.isEmpty()) {
            return songs;
        }
        List<Integer> songIds = songs.stream().map(SongCardDTO::getId).toList();
        Map<Integer, Integer> groupIds = songLinkService.getGroupIdsForSongs(songIds);
        Map<String, List<SongCardDTO>> grouped = new java.util.LinkedHashMap<>();
        for (SongCardDTO song : songs) {
            Integer groupId = groupIds.get(song.getId());
            String key = groupId != null ? "g:" + groupId : "s:" + song.getId();
            grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(song);
        }

        List<SongCardDTO> combined = new ArrayList<>();
        for (List<SongCardDTO> group : grouped.values()) {
            if (group.size() == 1) {
                SongCardDTO single = group.get(0);
                single.setLinkedSongGroup(false);
                single.setLinkedSongCount(1);
                combined.add(single);
                continue;
            }

                SongCardDTO representative = songLinkService.chooseRepresentativeSong(group, SongCardDTO::getId, SongCardDTO::getName);
                if (representative == null) {
                representative = group.get(0);
                }
            SongCardDTO dto = copySongCard(representative);
            dto.setPlayCount(group.stream().mapToInt(song -> song.getPlayCount() != null ? song.getPlayCount() : 0).sum());
            dto.setVatitoPlayCount(group.stream().mapToInt(song -> song.getVatitoPlayCount() != null ? song.getVatitoPlayCount() : 0).sum());
            dto.setRobertloverPlayCount(group.stream().mapToInt(song -> song.getRobertloverPlayCount() != null ? song.getRobertloverPlayCount() : 0).sum());
            long timeListened = group.stream().mapToLong(song -> song.getTimeListened() != null ? song.getTimeListened() : 0L).sum();
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            dto.setDaysListened(group.stream().map(SongCardDTO::getDaysListened).filter(java.util.Objects::nonNull).max(Integer::compareTo).orElse(0));
            dto.setWeeksListened(group.stream().map(SongCardDTO::getWeeksListened).filter(java.util.Objects::nonNull).max(Integer::compareTo).orElse(0));
            dto.setMonthsListened(group.stream().map(SongCardDTO::getMonthsListened).filter(java.util.Objects::nonNull).max(Integer::compareTo).orElse(0));
            dto.setYearsListened(group.stream().map(SongCardDTO::getYearsListened).filter(java.util.Objects::nonNull).max(Integer::compareTo).orElse(0));
            dto.setLinkedSongGroup(true);
            dto.setLinkedSongCount(group.size());
            dto.setTotalPlayBreakdownItems(buildSongPlayBreakdownItems(group, SongCardDTO::getPlayCount, false));
            dto.setPrimaryPlayBreakdownItems(buildSongPlayBreakdownItems(group, SongCardDTO::getVatitoPlayCount, true));
            dto.setLegacyPlayBreakdownItems(buildSongPlayBreakdownItems(group, SongCardDTO::getRobertloverPlayCount, true));
            combined.add(dto);
        }

        java.util.Comparator<SongCardDTO> combinedComparator = songCardComparator(sortBy, sortDirection);
        if (sortBy2 != null && !sortBy2.isBlank()) {
            combinedComparator = combinedComparator.thenComparing(songCardComparator(sortBy2, sortDirection2));
        }
        if (sortBy3 != null && !sortBy3.isBlank()) {
            combinedComparator = combinedComparator.thenComparing(songCardComparator(sortBy3, sortDirection3));
        }
        combinedComparator = combinedComparator
            .thenComparing(java.util.Comparator.comparing(
                SongCardDTO::getPlayCount,
                java.util.Comparator.nullsFirst(Integer::compareTo)
            ).reversed())
            .thenComparing(SongCardDTO::getName, String.CASE_INSENSITIVE_ORDER);
        combined.sort(combinedComparator);
        return combined;
    }

    private java.util.Comparator<SongCardDTO> songCardComparator(String sortBy, String sortDirection) {
        String normalizedSortBy = (sortBy == null || sortBy.isBlank()) ? "name" : sortBy;
        boolean desc = "desc".equalsIgnoreCase(sortDirection);
        return switch (normalizedSortBy) {
            case "artist" -> comparingStrings(SongCardDTO::getArtistName, desc, false);
            case "album" -> comparingStrings(SongCardDTO::getAlbumName, desc, false);
            case "plays" -> comparingComparable(SongCardDTO::getPlayCount, desc, false);
            case "primary_plays" -> comparingComparable(SongCardDTO::getVatitoPlayCount, desc, false);
            case "legacy_plays" -> comparingComparable(SongCardDTO::getRobertloverPlayCount, desc, false);
            case "time" -> comparingComparable(SongCardDTO::getTimeListened, desc, false);
            case "length" -> comparingComparable(SongCardDTO::getLengthSeconds, desc, false);
            case "release_date" -> comparingDisplayDates(SongCardDTO::getReleaseDate, desc, false);
            case "first_listened" -> comparingDisplayDates(SongCardDTO::getFirstListenedDate, desc, false);
            case "last_listened" -> comparingDisplayDates(SongCardDTO::getLastListenedDate, desc, false);
            case "days_listened" -> comparingComparable(SongCardDTO::getDaysListened, desc, false);
            case "weeks_listened" -> comparingComparable(SongCardDTO::getWeeksListened, desc, false);
            case "months_listened" -> comparingComparable(SongCardDTO::getMonthsListened, desc, false);
            case "years_listened" -> comparingComparable(SongCardDTO::getYearsListened, desc, false);
            case "track_number" -> comparingComparable(SongCardDTO::getTrackNumber, desc, false);
            case "age_at_release" -> comparingComparable(SongCardDTO::getAgeAtRelease, desc, false);
            case "featured_artist_count" -> comparingComparable(SongCardDTO::getFeaturedArtistCount, desc, false);
            case "seasonal_chart_peak" -> comparingComparable(SongCardDTO::getSeasonalChartPeak, desc, true)
                .thenComparing(comparingStrings(SongCardDTO::getSeasonalChartPeakPeriod, true, true));
            case "weekly_chart_peak" -> comparingComparable(SongCardDTO::getWeeklyChartPeak, desc, true)
                .thenComparing(comparingDisplayDates(SongCardDTO::getWeeklyChartPeakStartDate, true, true));
            case "weekly_chart_weeks" -> comparingComparable(SongCardDTO::getWeeklyChartWeeks, desc, false);
            case "trl_peak" -> comparingComparable(SongCardDTO::getTrlPeak, desc, true)
                .thenComparing(comparingComparable(SongCardDTO::getTrlDays, true, true));
            case "trl_days" -> comparingComparable(SongCardDTO::getTrlDays, desc, false);
            case "vatos_cuntdown_peak" -> comparingComparable(SongCardDTO::getVatosCuntdownPeak, desc, true)
                .thenComparing(comparingComparable(SongCardDTO::getVatosCuntdownDays, true, true));
            case "vatos_cuntdown_days" -> comparingComparable(SongCardDTO::getVatosCuntdownDays, desc, false);
            case "billboard_peak" -> comparingComparable(SongCardDTO::getBillboardPeak, desc, true)
                .thenComparing(comparingComparable(SongCardDTO::getBillboardWeeks, true, true));
            case "billboard_weeks" -> comparingComparable(SongCardDTO::getBillboardWeeks, desc, false);
            case "yearly_chart_peak" -> comparingComparable(SongCardDTO::getYearlyChartPeak, desc, true)
                .thenComparing(comparingStrings(SongCardDTO::getYearlyChartPeakPeriod, true, true));
            case "genre" -> comparingStrings(SongCardDTO::getGenreName, desc, false);
            case "subgenre" -> comparingStrings(SongCardDTO::getSubgenreName, desc, false);
            case "ethnicity" -> comparingStrings(SongCardDTO::getEthnicityName, desc, false);
            case "country" -> comparingStrings(SongCardDTO::getCountry, desc, false);
            case "language" -> comparingStrings(SongCardDTO::getLanguageName, desc, false);
            case "birth_date" -> comparingDisplayDates(SongCardDTO::getBirthDate, desc, false);
            case "death_date" -> comparingDisplayDates(SongCardDTO::getDeathDate, desc, false);
            case "image_count" -> comparingComparable(SongCardDTO::getImageCount, desc, false);
            default -> comparingStrings(SongCardDTO::getName, desc, false);
        };
    }

    private <T> java.util.Comparator<SongCardDTO> comparingValues(Function<SongCardDTO, T> extractor,
                                                                  java.util.Comparator<T> valueComparator,
                                                                  boolean desc,
                                                                  boolean forceNullsLast) {
        java.util.Comparator<T> directionalComparator = desc ? valueComparator.reversed() : valueComparator;
        java.util.Comparator<T> nullAwareComparator = forceNullsLast
                ? java.util.Comparator.nullsLast(directionalComparator)
                : (desc
                    ? java.util.Comparator.nullsLast(directionalComparator)
                    : java.util.Comparator.nullsFirst(directionalComparator));
        return java.util.Comparator.comparing(extractor, nullAwareComparator);
    }

    private <T extends Comparable<? super T>> java.util.Comparator<SongCardDTO> comparingComparable(Function<SongCardDTO, T> extractor,
                                                                                                     boolean desc,
                                                                                                     boolean forceNullsLast) {
        return comparingValues(extractor, java.util.Comparator.naturalOrder(), desc, forceNullsLast);
    }

    private java.util.Comparator<SongCardDTO> comparingStrings(Function<SongCardDTO, String> extractor,
                                                               boolean desc,
                                                               boolean forceNullsLast) {
        return comparingValues(extractor, String.CASE_INSENSITIVE_ORDER, desc, forceNullsLast);
    }

    private java.util.Comparator<SongCardDTO> comparingDisplayDates(Function<SongCardDTO, String> extractor,
                                                                    boolean desc,
                                                                    boolean forceNullsLast) {
        return comparingComparable(song -> parseDisplayDate(extractor.apply(song)), desc, forceNullsLast);
    }

    private LocalDate parseDisplayDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String normalizedValue = value.trim();
        if ("-".equals(normalizedValue)) {
            return null;
        }
        try {
            return LocalDate.parse(normalizedValue, DISPLAY_DATE_FORMATTER);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDate.parse(normalizedValue, LEGACY_DISPLAY_DATE_FORMATTER);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDate.parse(normalizedValue);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private SongCardDTO copySongCard(SongCardDTO source) {
        SongCardDTO target = new SongCardDTO();
        target.setId(source.getId());
        target.setName(source.getName());
        target.setArtistName(source.getArtistName());
        target.setArtistId(source.getArtistId());
        target.setAlbumName(source.getAlbumName());
        target.setAlbumId(source.getAlbumId());
        target.setGenreId(source.getGenreId());
        target.setGenreName(source.getGenreName());
        target.setSubgenreId(source.getSubgenreId());
        target.setSubgenreName(source.getSubgenreName());
        target.setLanguageId(source.getLanguageId());
        target.setLanguageName(source.getLanguageName());
        target.setEthnicityId(source.getEthnicityId());
        target.setEthnicityName(source.getEthnicityName());
        target.setGenderId(source.getGenderId());
        target.setGenderName(source.getGenderName());
        target.setReleaseYear(source.getReleaseYear());
        target.setReleaseDate(source.getReleaseDate());
        target.setFirstListenedDate(source.getFirstListenedDate());
        target.setLastListenedDate(source.getLastListenedDate());
        target.setTrackNumber(source.getTrackNumber());
        target.setLengthSeconds(source.getLengthSeconds());
        target.setLengthFormatted(source.getLengthFormatted());
        target.setHasImage(source.getHasImage());
        target.setAlbumHasImage(source.getAlbumHasImage());
        target.setCountry(source.getCountry());
        target.setOrganized(source.getOrganized());
        target.setIsSingle(source.getIsSingle());
        target.setInItunes(source.getInItunes());
        target.setBirthDate(source.getBirthDate());
        target.setDeathDate(source.getDeathDate());
        target.setImageCount(source.getImageCount());
        target.setBillboardPeak(source.getBillboardPeak());
        target.setBillboardWeeks(source.getBillboardWeeks());
        target.setSeasonalChartPeak(source.getSeasonalChartPeak());
        target.setTrlDays(source.getTrlDays());
        target.setTrlPeak(source.getTrlPeak());
        target.setVatosCuntdownDays(source.getVatosCuntdownDays());
        target.setVatosCuntdownPeak(source.getVatosCuntdownPeak());
        target.setWeeklyChartPeak(source.getWeeklyChartPeak());
        target.setWeeklyChartWeeks(source.getWeeklyChartWeeks());
        target.setYearlyChartPeak(source.getYearlyChartPeak());
        target.setWeeklyChartPeakStartDate(source.getWeeklyChartPeakStartDate());
        target.setSeasonalChartPeakPeriod(source.getSeasonalChartPeakPeriod());
        target.setYearlyChartPeakPeriod(source.getYearlyChartPeakPeriod());
        target.setWeeklyChartPeakWeeks(source.getWeeklyChartPeakWeeks());
        target.setSeasonalChartPeakSeasons(source.getSeasonalChartPeakSeasons());
        target.setYearlyChartPeakYears(source.getYearlyChartPeakYears());
        target.setAgeAtRelease(source.getAgeAtRelease());
        target.setFeaturedArtistCount(source.getFeaturedArtistCount());
        target.setTotalPlayBreakdownItems(source.getTotalPlayBreakdownItems());
        target.setPrimaryPlayBreakdownItems(source.getPrimaryPlayBreakdownItems());
        target.setLegacyPlayBreakdownItems(source.getLegacyPlayBreakdownItems());
        return target;
    }

    private List<String> buildSongPlayBreakdownItems(List<SongCardDTO> group,
                                                     java.util.function.Function<SongCardDTO, Integer> countExtractor,
                                                     boolean suppressZeroCounts) {
        if (group == null || group.size() <= 1) {
            return List.of();
        }

        long distinctArtists = group.stream()
                .map(SongCardDTO::getArtistName)
                .filter(Objects::nonNull)
                .distinct()
                .count();
        boolean includeArtist = distinctArtists > 1;

        List<String> items = group.stream()
                .map(song -> formatSongPlayBreakdownItem(song, countExtractor.apply(song), includeArtist, suppressZeroCounts))
                .filter(item -> item != null && !item.isBlank())
                .toList();
        return items.size() > 1 ? items : List.of();
    }

    private String formatSongPlayBreakdownItem(SongCardDTO song,
                                               Integer count,
                                               boolean includeArtist,
                                               boolean suppressZeroCounts) {
        int safeCount = count != null ? count : 0;
        if (suppressZeroCounts && safeCount <= 0) {
            return null;
        }

        StringBuilder label = new StringBuilder();
        if (includeArtist && song.getArtistName() != null && !song.getArtistName().isBlank()) {
            label.append(song.getArtistName()).append(" - ");
        }
        label.append(song.getName() != null ? song.getName() : "Unknown song");
        if (song.getAlbumName() != null && !song.getAlbumName().isBlank()) {
            label.append(" (").append(song.getAlbumName()).append(")");
        }
        label.append(": ").append(String.format(Locale.US, "%,d", safeCount));
        return label.toString();
    }
    
    public long countSongs(String name, List<Integer> artistName, String albumName,
                          List<Integer> genreIds, String genreMode,
                          List<Integer> subgenreIds, String subgenreMode,
                          List<Integer> languageIds, String languageMode,
                          List<Integer> genderIds, String genderMode,
                          List<Integer> ethnicityIds, String ethnicityMode,
                          List<String> countries, String countryMode,
                          List<Integer> tagIds, String tagMode,
                          List<String> accounts, String accountMode,
                          String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                          String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                          String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                          String listenedDateFrom, String listenedDateTo,
                          String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                          Integer ageMin, Integer ageMax, String ageMode,
                          Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                          String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                          String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                          String itunesIdsJson, String inItunes,
                          Integer playCountMin, Integer playCountMax,
                          Integer trackNumber, String trackNumberMode,
                          Integer lengthMin, Integer lengthMax, String lengthMode,
                          Integer weeklyChartPeak, Integer weeklyChartWeeks,
                          String weeklyChartDateFrom, String weeklyChartDateTo, String weeklyChartSeason,
                          Integer trlPeak, Integer trlDays,
                          String trlDateFrom, String trlDateTo,
                          Integer vatosCuntdownPeak, Integer vatosCuntdownDays,
                          String vatosCuntdownDateFrom, String vatosCuntdownDateTo,
                          Integer billboardPeak, Integer billboardWeeks,
                          String billboardDateFrom, String billboardDateTo,
                          Integer seasonalChartPeak, Integer seasonalChartSeasons,
                          String seasonalChartDateFrom, String seasonalChartDateTo, String seasonalChartSeason,
                          Integer yearlyChartPeak, Integer yearlyChartYears,
                          String yearlyChartDateFrom, String yearlyChartDateTo) {
        // Normalize empty lists to null to avoid native SQL IN () syntax errors in SQLite
        if (accounts != null && accounts.isEmpty()) accounts = null;
        if (tagIds != null && tagIds.isEmpty()) tagIds = null;
        
        if (appConfigService.isCombineLinkedSongsEnabled()) {
            String combinedSongsCacheKey = buildCombinedSongsFilterCacheKey(
                    name, artistName, albumName,
                    genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                    genderIds, genderMode, ethnicityIds, ethnicityMode, countries, countryMode, tagIds, tagMode, accounts, accountMode,
                    releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                    firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                    lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                    listenedDateFrom, listenedDateTo,
                    organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                    ageMin, ageMax, ageMode,
                    ageAtReleaseMin, ageAtReleaseMax,
                    birthDate, birthDateFrom, birthDateTo, birthDateMode,
                    deathDate, deathDateFrom, deathDateTo, deathDateMode,
                    itunesIdsJson, inItunes,
                    playCountMin, playCountMax,
                    trackNumber, trackNumberMode,
                    lengthMin, lengthMax, lengthMode,
                    weeklyChartPeak, weeklyChartWeeks,
                        weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                    trlPeak, trlDays,
                        trlDateFrom, trlDateTo,
                    vatosCuntdownPeak, vatosCuntdownDays,
                        vatosCuntdownDateFrom, vatosCuntdownDateTo,
                    billboardPeak, billboardWeeks,
                        billboardDateFrom, billboardDateTo,
                        seasonalChartPeak, seasonalChartSeasons,
                        seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason,
                        yearlyChartPeak, yearlyChartYears,
                        yearlyChartDateFrom, yearlyChartDateTo
            );
            Long cachedCount = getCachedCombinedSongCount(combinedSongsCacheKey);
            if (cachedCount != null) {
                return cachedCount;
            }

            List<Object[]> results = songRepository.findSongsWithStats(
                    name, artistName, albumName,
                    genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                    genderIds, genderMode, ethnicityIds, ethnicityMode, countries, countryMode, tagIds, tagMode, accounts, accountMode,
                    releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                    firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                    lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                    listenedDateFrom, listenedDateTo,
                    organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                    itunesIdsJson, inItunes,
                    ageMin, ageMax, ageMode,
                    ageAtReleaseMin, ageAtReleaseMax,
                    birthDate, birthDateFrom, birthDateTo, birthDateMode,
                    deathDate, deathDateFrom, deathDateTo, deathDateMode,
                    playCountMin, playCountMax,
                    trackNumber, trackNumberMode,
                    lengthMin, lengthMax, lengthMode,
                    weeklyChartPeak, weeklyChartWeeks,
                        weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                    trlPeak, trlDays,
                        trlDateFrom, trlDateTo,
                    vatosCuntdownPeak, vatosCuntdownDays,
                        vatosCuntdownDateFrom, vatosCuntdownDateTo,
                    billboardPeak, billboardWeeks,
                        billboardDateFrom, billboardDateTo,
                        seasonalChartPeak, seasonalChartSeasons,
                        seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason,
                        yearlyChartPeak, yearlyChartYears,
                        yearlyChartDateFrom, yearlyChartDateTo,
                    "plays", "desc", null, null, null, null, 100000, 0
            );
                    long combinedCount = countCombinedRows(results);
                    cacheCombinedSongCount(combinedSongsCacheKey, combinedCount);
                    return combinedCount;
        }

        return songRepository.countSongsWithFilters(name, artistName, albumName, 
                genreIds, genreMode, subgenreIds, subgenreMode, languageIds, languageMode,
                genderIds, genderMode, ethnicityIds, ethnicityMode, countries, countryMode, tagIds, tagMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                itunesIdsJson, inItunes,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks,
                weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                trlPeak, trlDays,
                trlDateFrom, trlDateTo,
                vatosCuntdownPeak, vatosCuntdownDays,
                vatosCuntdownDateFrom, vatosCuntdownDateTo,
                billboardPeak, billboardWeeks,
                billboardDateFrom, billboardDateTo,
                seasonalChartPeak, seasonalChartSeasons,
                seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears,
                yearlyChartDateFrom, yearlyChartDateTo);
    }
    
    /**
     * Count songs by gender for the filtered dataset.
     * Returns a GenderCountDTO with male, female, and other counts.
     * Uses efficient SQL GROUP BY instead of loading all records.
     */
    public GenderCountDTO countSongsByGender(String name, List<Integer> artistName, String albumName,
                          List<Integer> genreIds, String genreMode,
                          List<Integer> subgenreIds, String subgenreMode,
                          List<Integer> languageIds, String languageMode,
                          List<Integer> genderIds, String genderMode,
                          List<Integer> ethnicityIds, String ethnicityMode,
                          List<String> countries, String countryMode,
                          List<Integer> tagIds, String tagMode,
                          List<String> accounts, String accountMode,
                          String releaseDate, String releaseDateFrom, String releaseDateTo, String releaseDateMode,
                          String firstListenedDate, String firstListenedDateFrom, String firstListenedDateTo, String firstListenedDateMode,
                          String lastListenedDate, String lastListenedDateFrom, String lastListenedDateTo, String lastListenedDateMode,
                          String listenedDateFrom, String listenedDateTo,
                          String organized, Integer imageCountMin, Integer imageCountMax, String hasFeaturedArtists, String isBand, String isSingle,
                          Integer ageMin, Integer ageMax, String ageMode,
                          Integer ageAtReleaseMin, Integer ageAtReleaseMax,
                          String birthDate, String birthDateFrom, String birthDateTo, String birthDateMode,
                          String deathDate, String deathDateFrom, String deathDateTo, String deathDateMode,
                          String itunesIdsJson, String inItunes,
                          Integer playCountMin, Integer playCountMax,
                          Integer trackNumber, String trackNumberMode,
                          Integer lengthMin, Integer lengthMax, String lengthMode,
                          Integer weeklyChartPeak, Integer weeklyChartWeeks,
                          String weeklyChartDateFrom, String weeklyChartDateTo, String weeklyChartSeason,
                          Integer trlPeak, Integer trlDays,
                          String trlDateFrom, String trlDateTo,
                          Integer vatosCuntdownPeak, Integer vatosCuntdownDays,
                          String vatosCuntdownDateFrom, String vatosCuntdownDateTo,
                          Integer billboardPeak, Integer billboardWeeks,
                          String billboardDateFrom, String billboardDateTo,
                          Integer seasonalChartPeak, Integer seasonalChartSeasons,
                          String seasonalChartDateFrom, String seasonalChartDateTo, String seasonalChartSeason,
                          Integer yearlyChartPeak, Integer yearlyChartYears,
                          String yearlyChartDateFrom, String yearlyChartDateTo) {
        // Normalize empty lists to null
        if (accounts != null && accounts.isEmpty()) accounts = null;
        if (tagIds != null && tagIds.isEmpty()) tagIds = null;

        // Use efficient SQL-based counting with GROUP BY
        Map<Integer, Long> genderCounts = songRepository.countSongsByGenderWithFilters(
                name, artistName, albumName, genreIds, genreMode,
                subgenreIds, subgenreMode, languageIds, languageMode, genderIds, genderMode,
                ethnicityIds, ethnicityMode, countries, countryMode, tagIds, tagMode, accounts, accountMode,
                releaseDate, releaseDateFrom, releaseDateTo, releaseDateMode,
                firstListenedDate, firstListenedDateFrom, firstListenedDateTo, firstListenedDateMode,
                lastListenedDate, lastListenedDateFrom, lastListenedDateTo, lastListenedDateMode,
                listenedDateFrom, listenedDateTo,
                organized, imageCountMin, imageCountMax, hasFeaturedArtists, isBand, isSingle,
                ageMin, ageMax, ageMode,
                ageAtReleaseMin, ageAtReleaseMax,
                birthDate, birthDateFrom, birthDateTo, birthDateMode,
                deathDate, deathDateFrom, deathDateTo, deathDateMode,
                itunesIdsJson, inItunes,
                playCountMin, playCountMax,
                trackNumber, trackNumberMode,
                lengthMin, lengthMax, lengthMode,
                weeklyChartPeak, weeklyChartWeeks,
                weeklyChartDateFrom, weeklyChartDateTo, weeklyChartSeason,
                trlPeak, trlDays,
                trlDateFrom, trlDateTo,
                vatosCuntdownPeak, vatosCuntdownDays,
                vatosCuntdownDateFrom, vatosCuntdownDateTo,
                billboardPeak, billboardWeeks,
                billboardDateFrom, billboardDateTo,
                seasonalChartPeak, seasonalChartSeasons,
                seasonalChartDateFrom, seasonalChartDateTo, seasonalChartSeason,
                yearlyChartPeak, yearlyChartYears,
                yearlyChartDateFrom, yearlyChartDateTo);
        
        // Gender ID 1 = Female, Gender ID 2 = Male
        long femaleCount = genderCounts.getOrDefault(1, 0L);
        long maleCount = genderCounts.getOrDefault(2, 0L);
        long otherCount = 0L;
        
        // Sum up all other gender IDs (including null)
        for (Map.Entry<Integer, Long> entry : genderCounts.entrySet()) {
            Integer genderId = entry.getKey();
            if (genderId == null || (genderId != 1 && genderId != 2)) {
                otherCount += entry.getValue();
            }
        }
        
        return new GenderCountDTO(maleCount, femaleCount, otherCount);
    }
    
    public Optional<Song> getSongById(Integer id) {
        String sql = """
            SELECT s.id, s.artist_id, s.album_id, s.name, s.length_seconds, s.is_single,
                   s.track_number, s.override_genre_id, s.override_subgenre_id, s.override_language_id,
                   s.override_gender_id, s.override_ethnicity_id, s.release_date, s.organized,
                   s.creation_date, s.update_date,
                   al.override_genre_id as album_genre_id,
                   al.override_subgenre_id as album_subgenre_id,
                   al.override_language_id as album_language_id,
                   ar.genre_id as artist_genre_id,
                   ar.subgenre_id as artist_subgenre_id,
                   ar.language_id as artist_language_id,
                   ar.gender_id as artist_gender_id,
                   ar.ethnicity_id as artist_ethnicity_id
            FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album al ON s.album_id = al.id
            WHERE s.id = ?
            """;
        
        List<Song> results = jdbcTemplate.query(sql, (rs, rowNum) -> {
            Song song = new Song();
            song.setId(rs.getInt("id"));
            song.setArtistId(rs.getInt("artist_id"));
            
            int albumId = rs.getInt("album_id");
            song.setAlbumId(rs.wasNull() ? null : albumId);
            
            song.setName(rs.getString("name"));
            
            int length = rs.getInt("length_seconds");
            song.setLengthSeconds(rs.wasNull() ? null : length);
            
            song.setIsSingle(rs.getInt("is_single") == 1);
            
            int trackNum = rs.getInt("track_number");
            song.setTrackNumber(rs.wasNull() ? null : trackNum);
            
            int genreId = rs.getInt("override_genre_id");
            song.setOverrideGenreId(rs.wasNull() ? null : genreId);
            
            int subgenreId = rs.getInt("override_subgenre_id");
            song.setOverrideSubgenreId(rs.wasNull() ? null : subgenreId);
            
            int languageId = rs.getInt("override_language_id");
            song.setOverrideLanguageId(rs.wasNull() ? null : languageId);
            
            int genderId = rs.getInt("override_gender_id");
            song.setOverrideGenderId(rs.wasNull() ? null : genderId);
            
            int ethnicityId = rs.getInt("override_ethnicity_id");
            song.setOverrideEthnicityId(rs.wasNull() ? null : ethnicityId);
            
            int organizedVal = rs.getInt("organized");
            song.setOrganized(rs.wasNull() ? null : organizedVal == 1);
            
            // Set inherited values from Album
            int albumGenreId = rs.getInt("album_genre_id");
            song.setAlbumGenreId(rs.wasNull() ? null : albumGenreId);
            
            int albumSubgenreId = rs.getInt("album_subgenre_id");
            song.setAlbumSubgenreId(rs.wasNull() ? null : albumSubgenreId);
            
            int albumLanguageId = rs.getInt("album_language_id");
            song.setAlbumLanguageId(rs.wasNull() ? null : albumLanguageId);
            
            // Set inherited values from Artist
            int artistGenreId = rs.getInt("artist_genre_id");
            song.setArtistGenreId(rs.wasNull() ? null : artistGenreId);
            
            int artistSubgenreId = rs.getInt("artist_subgenre_id");
            song.setArtistSubgenreId(rs.wasNull() ? null : artistSubgenreId);
            
            int artistLanguageId = rs.getInt("artist_language_id");
            song.setArtistLanguageId(rs.wasNull() ? null : artistLanguageId);
            
            int artistGenderId = rs.getInt("artist_gender_id");
            song.setArtistGenderId(rs.wasNull() ? null : artistGenderId);
            
            int artistEthnicityId = rs.getInt("artist_ethnicity_id");
            song.setArtistEthnicityId(rs.wasNull() ? null : artistEthnicityId);
            
            // Try multiple methods to read release_date from SQLite
            java.sql.Date releaseDate = null;
            try {
                // Try getting as Date object first
                releaseDate = rs.getDate("release_date");
            } catch (Exception e1) {
                // If that fails, try as string
                try {
                    String releaseDateStr = rs.getString("release_date");
                    if (releaseDateStr != null && !releaseDateStr.isEmpty()) {
                        // Try parsing as long (timestamp)
                        try {
                            long timestamp = Long.parseLong(releaseDateStr);
                            releaseDate = new java.sql.Date(timestamp);
                        } catch (NumberFormatException e2) {
                            // Try parsing as date string
                            releaseDate = parseDate(releaseDateStr);
                        }
                    }
                } catch (Exception e2) {
                    // Date parsing failed, leave as null
                }
            }
            song.setReleaseDate(releaseDate);
            
            // Robust timestamp parsing for SQLite date-only values
            String creation = rs.getString("creation_date");
            String update = rs.getString("update_date");
            song.setCreationDate(parseTimestamp(creation));
            song.setUpdateDate(parseTimestamp(update));
            
            return song;
        }, id);
        
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }
    
    public Song saveSong(Song song) {
        // Get the old song data to check if name or album changed
        String oldName = null;
        Integer oldAlbumId = null;
        try {
            Map<String, Object> oldData = jdbcTemplate.queryForMap(
                "SELECT name, album_id FROM Song WHERE id = ?", song.getId());
            oldName = (String) oldData.get("name");
            oldAlbumId = oldData.get("album_id") != null ? ((Number) oldData.get("album_id")).intValue() : null;
        } catch (Exception e) {
            // Ignore if song doesn't exist
        }
        
        String sql = """
            UPDATE Song 
            SET name = ?, artist_id = ?, album_id = ?, release_date = ?,
                length_seconds = ?, track_number = ?, is_single = ?,
                override_genre_id = ?, override_subgenre_id = ?, override_language_id = ?,
                override_gender_id = ?, override_ethnicity_id = ?, organized = ?,
                update_date = CURRENT_TIMESTAMP
            WHERE id = ?
            """;
        
        // Convert java.sql.Date to yyyy-MM-dd string format for database
        String releaseDateStr = null;
        if (song.getReleaseDate() != null) {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            releaseDateStr = sdf.format(song.getReleaseDate());
        }
        
        jdbcTemplate.update(sql, 
            song.getName(),
            song.getArtistId(),
            song.getAlbumId(),
            releaseDateStr,
            song.getLengthSeconds(),
            song.getTrackNumber(),
            song.getIsSingle() ? 1 : 0,
            song.getOverrideGenreId(),
            song.getOverrideSubgenreId(),
            song.getOverrideLanguageId(),
            song.getOverrideGenderId(),
            song.getOverrideEthnicityId(),
            song.getOrganized() != null && song.getOrganized() ? 1 : 0,
            song.getId()
        );
        
        // If song name changed, update plays
        if (oldName != null && !oldName.equals(song.getName())) {
            updatePlaysForSongNameChange(song.getId(), song.getName());
        }
        
        // If album changed, update plays with new album name
        boolean albumChanged = (oldAlbumId == null && song.getAlbumId() != null) ||
                              (oldAlbumId != null && !oldAlbumId.equals(song.getAlbumId()));
        if (albumChanged) {
            String newAlbumName = song.getAlbumId() != null ? getAlbumName(song.getAlbumId()) : null;
            updatePlaysForSongAlbumChange(song.getId(), newAlbumName);
        }
        
        // Only try to match unmatched plays if name or album changed (expensive operation)
        boolean nameChanged = oldName != null && !oldName.equals(song.getName());
        if (nameChanged || albumChanged) {
            tryMatchUnmatchedPlaysForSong(song);
        }
        
        return song;
    }
    
    /**
     * Update the song name in all plays for this song
     */
    private void updatePlaysForSongNameChange(int songId, String newSongName) {
        String sql = "UPDATE Play SET song = ? WHERE song_id = ?";
        jdbcTemplate.update(sql, newSongName, songId);
    }
    
    /**
     * Update the album name in all plays for this song
     */
    private void updatePlaysForSongAlbumChange(int songId, String newAlbumName) {
        String sql = "UPDATE Play SET album = ? WHERE song_id = ?";
        jdbcTemplate.update(sql, newAlbumName, songId);
    }
    
    /**
     * Try to match unmatched plays to this song.
     * Looks for plays where artist, album, and song name match.
     */
    private void tryMatchUnmatchedPlaysForSong(Song song) {
        // Get artist name
        String artistName = getArtistName(song.getArtistId());
        if (artistName == null) return;
        
        // Get album name (may be null for singles)
        String albumName = song.getAlbumId() != null ? getAlbumName(song.getAlbumId()) : null;
        
        // Try to match unmatched plays and update their canonical names
        String sql = """
            UPDATE Play 
            SET song_id = ?,
                artist = ?,
                album = ?,
                song = ?
            WHERE song_id IS NULL
            AND LOWER(COALESCE(artist, '')) = LOWER(?)
            AND LOWER(COALESCE(album, '')) = LOWER(?)
            AND LOWER(COALESCE(song, '')) = LOWER(?)
            """;
        
        jdbcTemplate.update(sql, 
            song.getId(),
            artistName,
            albumName != null ? albumName : "",
            song.getName(),
            artistName,
            albumName != null ? albumName : "",
            song.getName()
        );
    }
    
    public void updateSongImage(Integer id, byte[] imageData) {
        String sql = "UPDATE Song SET single_cover = ? WHERE id = ?";
        jdbcTemplate.update(sql, imageData, id);
    }
    
    public byte[] getSongImage(Integer id) {
        // Priority: 1) single_cover, 2) first SongImage gallery image, 3) album image
        String sql = """
            SELECT COALESCE(
                s.single_cover,
                (SELECT image FROM SongImage WHERE song_id = s.id ORDER BY display_order ASC LIMIT 1),
                a.image
            ) as image
            FROM Song s
            LEFT JOIN Album a ON s.album_id = a.id
            WHERE s.id = ?
            """;
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("image"), id);
        } catch (Exception e) {
            return null;
        }
    }
    
    // Get only the song's own image (single_cover), not falling back to album
    public byte[] getSongOwnImage(Integer id) {
        String sql = "SELECT single_cover FROM Song WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> rs.getBytes("single_cover"), id);
        } catch (Exception e) {
            return null;
        }
    }

    // Gallery methods for secondary images
    // If song has no single_cover, the first SongImage is used as the default image,
    // so we exclude it from the gallery list to avoid showing it twice
    public List<SongImage> getSecondaryImages(Integer songId) {
        List<SongImage> allImages = songImageRepository.findBySongIdOrderByDisplayOrderAsc(songId);
        
        // Check if the song has its own single_cover
        byte[] singleCover = getSongOwnImage(songId);
        boolean hasSingleCover = singleCover != null && singleCover.length > 0;
        
        // If no single_cover and there are gallery images, the first one is the "default"
        // so we skip it to avoid duplication in the gallery
        if (!hasSingleCover && allImages.size() > 0) {
            return allImages.subList(1, allImages.size());
        }
        
        return allImages;
    }

    public int getSecondaryImageCount(Integer songId) {
        return songImageRepository.countBySongId(songId);
    }

    public byte[] getSecondaryImage(Integer imageId) {
        return songImageRepository.findById(imageId)
                .map(SongImage::getImage)
                .orElse(null);
    }

    /**
     * Check if an image already exists for this song (either as single_cover or in gallery).
     * Uses byte length first for speed, then compares actual data.
     */
    public boolean isDuplicateImage(Integer songId, byte[] imageData) {
        if (imageData == null || imageData.length == 0) return false;
        
        // Check against single_cover
        byte[] singleCover = getSongOwnImage(songId);
        if (singleCover != null && singleCover.length == imageData.length && java.util.Arrays.equals(singleCover, imageData)) {
            return true;
        }
        
        // Check against gallery images
        List<SongImage> galleryImages = songImageRepository.findBySongIdOrderByDisplayOrderAsc(songId);
        for (SongImage existing : galleryImages) {
            byte[] existingData = existing.getImage();
            if (existingData != null && existingData.length == imageData.length && java.util.Arrays.equals(existingData, imageData)) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Add a secondary image to the song gallery.
     * @return true if image was added, false if it was a duplicate and skipped
     */
    public boolean addSecondaryImage(Integer songId, byte[] imageData) {
        // Check for duplicates first
        if (isDuplicateImage(songId, imageData)) {
            return false; // Skip duplicate
        }
        
        Integer maxOrder = songImageRepository.getMaxDisplayOrder(songId);
        SongImage image = new SongImage();
        image.setSongId(songId);
        image.setImage(imageData);
        image.setDisplayOrder(maxOrder + 1);
        image.setCreationDate(new java.sql.Timestamp(System.currentTimeMillis()));
        songImageRepository.save(image);
        return true;
    }

    @Transactional
    public void deleteSecondaryImage(Integer imageId) {
        songImageRepository.deleteById(imageId);
    }

    @Transactional
    public void swapToDefault(Integer songId, Integer imageId) {
        // Get the current default image from the main Song table (single_cover only, not album fallback)
        byte[] currentDefault = getSongOwnImage(songId);

        // Get the secondary image to promote
        SongImage secondaryImage = songImageRepository.findById(imageId)
                .orElseThrow(() -> new IllegalArgumentException("Image not found: " + imageId));

        // Set the secondary image as the new default
        updateSongImage(songId, secondaryImage.getImage());

        // If there was a previous default, move it to secondary images
        if (currentDefault != null && currentDefault.length > 0) {
            // Update the secondary image record with the old default
            secondaryImage.setImage(currentDefault);
            songImageRepository.save(secondaryImage);
        } else {
            // No previous default, just delete the secondary record
            songImageRepository.deleteById(imageId);
        }
    }

    public String getArtistName(int artistId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM Artist WHERE id = ?", String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public String getAlbumName(int albumId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM Album WHERE id = ?", String.class, albumId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public String getArtistGender(Integer artistId) {
        String sql = "SELECT g.name FROM Artist a LEFT JOIN Gender g ON a.gender_id = g.id WHERE a.id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public String getArtistCountry(Integer artistId) {
        String sql = "SELECT country FROM Artist WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, artistId);
        } catch (Exception e) {
            return null;
        }
    }
    
    public Map<Integer, String> getGenres() {
        return lookupRepository.getAllGenres();
    }
    
    public Map<Integer, String> getSubGenres() {
        return lookupRepository.getAllSubGenres();
    }
    
    public Map<Integer, String> getLanguages() {
        return lookupRepository.getAllLanguages();
    }
    
    public Map<Integer, String> getGenders() {
        return lookupRepository.getAllGenders();
    }
    
    public Map<Integer, String> getEthnicities() {
        return lookupRepository.getAllEthnicities();
    }
    
    /**
     * Get only the distinct countries that exist in the Artist table (for filters)
     */
    public List<String> getCountries() {
        String sql = "SELECT DISTINCT country FROM Artist WHERE country IS NOT NULL AND country != '' ORDER BY country";
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    // Helper to parse various SQLite timestamp representations
    private static java.sql.Timestamp parseTimestamp(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        try {
            if (v.length() == 10 && v.matches("\\d{4}-\\d{2}-\\d{2}")) {
                v = v + " 00:00:00";
            } else if (v.contains("T") && v.matches("\\d{4}-\\d{2}-\\d{2}T.*")) {
                v = v.replace('T', ' ');
            }
            return java.sql.Timestamp.valueOf(v);
        } catch (Exception e) {
            try {
                if (v.length() >= 10) {
                    String datePart = v.substring(0, 10);
                    if (datePart.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        return java.sql.Timestamp.valueOf(datePart + " 00:00:00");
                    }
                }
            } catch (Exception ignore) {}
            return null;
        }
    }

    // Helper to parse date values from database (yyyy-MM-dd format)
    private static java.sql.Date parseDate(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        
        // Parse as date string
        if (v.contains("T")) v = v.replace('T', ' ');
        if (v.length() >= 10) v = v.substring(0, 10);
        if (v.matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return java.sql.Date.valueOf(v);
            } catch (Exception ignore) {}
        }
        return null;
    }

    // NEW: total plays for a song (count plays for this song)
    public int getPlayCountForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Play WHERE song_id IN (" + placeholders(ids) + ")",
                Integer.class, ids.toArray());
        return count != null ? count : 0;
    }

    // Get vatito (primary) play count for song
    public int getVatitoPlayCountForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        Object[] params = append(ids, "vatito");
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Play WHERE song_id IN (" + placeholders(ids) + ") AND account = ?",
                Integer.class, params);
        return count != null ? count : 0;
    }
    
    // Get robertlover (legacy) play count for song
    public int getRobertloverPlayCountForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        Object[] params = append(ids, "robertlover");
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM Play WHERE song_id IN (" + placeholders(ids) + ") AND account = ?",
                Integer.class, params);
        return count != null ? count : 0;
    }

    // Return a string with per-account play counts for this song (e.g. "lastfm: 12\nspotify: 3\n")
    public String getPlaysByAccountForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT account, COUNT(*) as cnt FROM Play WHERE song_id IN (" + placeholders(ids) + ") GROUP BY account ORDER BY cnt DESC";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, ids.toArray());
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> row : rows) {
            Object account = row.get("account");
            Object cnt = row.get("cnt");
            sb.append(account != null ? account.toString() : "unknown");
            sb.append(": ");
            sb.append(cnt != null ? cnt.toString() : "0");
            sb.append("\n");
        }
        return sb.toString();
    }

    // Get total listening time for a song
    public String getTotalListeningTimeForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = """
            SELECT SUM(COALESCE(s.length_seconds, 0)) as total_seconds
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            WHERE p.song_id IN (%s)
            """;
        
        try {
            Integer totalSeconds = jdbcTemplate.queryForObject(sql.formatted(placeholders(ids)), Integer.class, ids.toArray());
            if (totalSeconds == null || totalSeconds == 0) {
                return "-";
            }
            return formatDuration(totalSeconds);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get first listened date for a song
    public String getFirstListenedDateForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT MIN(play_date) FROM Play WHERE song_id IN (" + placeholders(ids) + ")";
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, ids.toArray());
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get first listened date for a song as LocalDate (for calculations)
    public java.time.LocalDate getFirstListenedDateAsLocalDateForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT MIN(DATE(play_date)) FROM Play WHERE song_id IN (" + placeholders(ids) + ")";
        try {
            String dateStr = jdbcTemplate.queryForObject(sql, String.class, ids.toArray());
            return dateStr != null ? java.time.LocalDate.parse(dateStr) : null;
        } catch (Exception e) {
            return null;
        }
    }

    // Get last listened date for a song
    public String getLastListenedDateForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT MAX(play_date) FROM Play WHERE song_id IN (" + placeholders(ids) + ")";
        try {
            String date = jdbcTemplate.queryForObject(sql, String.class, ids.toArray());
            return formatDate(date);
        } catch (Exception e) {
            return "-";
        }
    }

    // Get unique days played for a song
    public int getUniqueDaysPlayedForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT COUNT(DISTINCT DATE(play_date)) FROM Play WHERE song_id IN (" + placeholders(ids) + ") AND play_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, ids.toArray());
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique weeks played for a song
    public int getUniqueWeeksPlayedForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT COUNT(DISTINCT strftime('%Y-%W', play_date)) FROM Play WHERE song_id IN (" + placeholders(ids) + ") AND play_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, ids.toArray());
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique months played for a song
    public int getUniqueMonthsPlayedForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT COUNT(DISTINCT strftime('%Y-%m', play_date)) FROM Play WHERE song_id IN (" + placeholders(ids) + ") AND play_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, ids.toArray());
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // Get unique years played for a song
    public int getUniqueYearsPlayedForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT COUNT(DISTINCT strftime('%Y', play_date)) FROM Play WHERE song_id IN (" + placeholders(ids) + ") AND play_date IS NOT NULL";
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, ids.toArray());
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    public List<Integer> getEffectiveSongIdsForStats(int songId) {
        if (!appConfigService.isCombineLinkedSongsEnabled()) {
            return List.of(songId);
        }
        return songLinkService.getLinkedSongIds(songId);
    }

    private String placeholders(List<Integer> ids) {
        return String.join(",", ids.stream().map(id -> "?").toList());
    }

    private Object[] append(List<Integer> ids, Object value) {
        Object[] params = new Object[ids.size() + 1];
        for (int i = 0; i < ids.size(); i++) {
            params[i] = ids.get(i);
        }
        params[ids.size()] = value;
        return params;
    }

    // Delete song (only if play count is 0)
    public void deleteSong(Integer songId) {
        // First check if song has any plays
        int playCount = getPlayCountForSong(songId);
        if (playCount > 0) {
            throw new IllegalStateException("Cannot delete song with existing plays");
        }
        
        // Delete the song
        jdbcTemplate.update("DELETE FROM Song WHERE id = ?", songId);
    }
    
    // Create a new song
    public Song createSong(Song song) {
        String sql = """
            INSERT INTO Song (artist_id, album_id, name, release_date, length_seconds, track_number,
                             is_single, override_genre_id, override_subgenre_id, 
                             override_language_id, override_gender_id, override_ethnicity_id,
                             creation_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """;
        
        // Convert java.sql.Date to yyyy-MM-dd string format for database
        String releaseDateStr = null;
        if (song.getReleaseDate() != null) {
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            releaseDateStr = sdf.format(song.getReleaseDate());
        }
        
        jdbcTemplate.update(sql,
            song.getArtistId(),
            song.getAlbumId(),
            song.getName(),
            releaseDateStr,
            song.getLengthSeconds(),
            song.getTrackNumber(),
            song.getIsSingle() != null && song.getIsSingle() ? 1 : 0,
            song.getOverrideGenreId(),
            song.getOverrideSubgenreId(),
            song.getOverrideLanguageId(),
            song.getOverrideGenderId(),
            song.getOverrideEthnicityId()
        );
        
        // Get the ID of the newly created song
        Integer id = jdbcTemplate.queryForObject("SELECT last_insert_rowid()", Integer.class);
        song.setId(id);
        
        return song;
    }
    
    // Create song from map (for API)
    public Integer createSong(java.util.Map<String, Object> data) {
        Song song = new Song();
        song.setName((String) data.get("name"));
        if (data.get("artistId") != null) {
            song.setArtistId(((Number) data.get("artistId")).intValue());
        }
        if (data.get("albumId") != null) {
            song.setAlbumId(((Number) data.get("albumId")).intValue());
        }
        if (data.get("releaseDate") != null) {
            // Convert dd/MM/yyyy to yyyy-MM-dd for parsing
            String dateStr = (String) data.get("releaseDate");
            if (dateStr != null && dateStr.matches("\\d{2}/\\d{2}/\\d{4}")) {
                String[] parts = dateStr.split("/");
                String isoDate = parts[2] + "-" + parts[1] + "-" + parts[0];
                try {
                    song.setReleaseDate(java.sql.Date.valueOf(isoDate));
                } catch (Exception ignore) {}
            }
        }
        if (data.get("lengthSeconds") != null) {
            song.setLengthSeconds(((Number) data.get("lengthSeconds")).intValue());
        }
        if (data.get("trackNumber") != null) {
            song.setTrackNumber(((Number) data.get("trackNumber")).intValue());
        }
        Song created = createSong(song);
        return created.getId();
    }
    
    // Helper method to format duration from seconds
    private String formatDuration(int totalSeconds) {
        if (totalSeconds <= 0) return "-";
        
        long days = totalSeconds / 86400;
        long hours = (totalSeconds % 86400) / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        
        if (days > 0) {
            return String.format("%dd:%02d:%02d:%02d", days, hours, minutes, seconds);
        } else if (hours > 0) {
            return String.format("%d:%02d:%02d", hours, minutes, seconds);
        } else {
            return String.format("%d:%02d", minutes, seconds);
        }
    }

    // Helper method to format date strings
    private String formatDate(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return "-";
        }
        
        try {
            String datePart = dateTimeString.trim();
            if (datePart.contains(" ")) {
                datePart = datePart.split(" ")[0];
            }
            
            String[] parts = datePart.split("-");
            if (parts.length == 3) {
                int year = Integer.parseInt(parts[0]);
                int month = Integer.parseInt(parts[1]);
                int day = Integer.parseInt(parts[2]);
                
                String[] monthNames = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
                
                // Format as DD-Mon-YYYY (e.g., "01-Nov-2025") with zero-padded day
                return String.format("%02d-%s-%d", day, monthNames[month - 1], year);
            }
        } catch (Exception e) {
            // If parsing fails, return as is
        }
        
        return dateTimeString;
    }
    
    /**
     * Apply iTunes filter to the ChartFilterDTO by setting songIds to only include songs
     * that match the inItunes criteria. This must be called before passing the filter
     * to repository methods.
     * 
     * @param filter the chart filter DTO with inItunes set
     */
    private void applyItunesFilter(ChartFilterDTO filter) {
        String inItunes = filter.getInItunes();
        String catalogType = normalizeCatalogType(filter.getCatalogType());
        
        // Populate itunesSongIdsJson if needed for presence ratio filtering
        if (filter.getItunesSongIdsJson() == null && 
                (filter.getItunesPresenceMin() != null || filter.getItunesPresenceMax() != null)) {
            filter.setItunesSongIdsJson(itunesService.getAllItunesSongIdsJson());
        }
        
        if (inItunes == null || inItunes.isEmpty()) {
            return; // No iTunes filter, nothing to do
        }
        
        boolean wantInItunes = "true".equalsIgnoreCase(inItunes);

                if ("artist".equals(catalogType)) {
                    applyArtistItunesFilter(filter, wantInItunes);
                    return;
                }

                if ("album".equals(catalogType)) {
                    applyAlbumItunesFilter(filter, wantInItunes);
                    return;
                }

                applySongItunesFilter(filter, wantInItunes);
    }

            private void applyArtistItunesFilter(ChartFilterDTO filter, boolean wantInItunes) {
                String sql = "SELECT id, name FROM Artist";
                List<Integer> matchingIds = new ArrayList<>();

                for (Map<String, Object> row : jdbcTemplate.queryForList(sql)) {
                    Integer artistId = (Integer) row.get("id");
                    String artistName = (String) row.get("name");
                    boolean existsInItunes = itunesService.artistExistsInItunes(artistName);
                    if (existsInItunes == wantInItunes) {
                        matchingIds.add(artistId);
                    }
                }

                filter.setArtistIds(intersectIds(matchingIds, filter.getArtistIds()));
            }

            private void applyAlbumItunesFilter(ChartFilterDTO filter, boolean wantInItunes) {
                String sql = """
                    SELECT alb.id, alb.name AS album_name, ar.name AS artist_name
                    FROM Album alb
                    INNER JOIN Artist ar ON alb.artist_id = ar.id
                    """;
                List<Integer> matchingIds = new ArrayList<>();

                for (Map<String, Object> row : jdbcTemplate.queryForList(sql)) {
                    Integer albumId = (Integer) row.get("id");
                    String artistName = (String) row.get("artist_name");
                    String albumName = (String) row.get("album_name");
                    boolean existsInItunes = itunesService.albumExistsInItunes(artistName, albumName);
                    if (existsInItunes == wantInItunes) {
                        matchingIds.add(albumId);
                    }
                }

                filter.setAlbumIds(intersectIds(matchingIds, filter.getAlbumIds()));
            }

            private void applySongItunesFilter(ChartFilterDTO filter, boolean wantInItunes) {
                String sql = """
                    SELECT s.id, ar.name as artist_name, COALESCE(alb.name, '') as album_name, s.name as song_name
                    FROM Song s
                    INNER JOIN Artist ar ON s.artist_id = ar.id
                    LEFT JOIN Album alb ON s.album_id = alb.id
                    """;

                List<Integer> matchingIds = new ArrayList<>();

                for (Map<String, Object> row : jdbcTemplate.queryForList(sql)) {
                    Integer songId = (Integer) row.get("id");
                    String artistName = (String) row.get("artist_name");
                    String albumName = (String) row.get("album_name");
                    String songName = (String) row.get("song_name");

                    boolean existsInItunes = itunesService.songExistsInItunes(artistName, albumName, songName);
                    if (existsInItunes == wantInItunes) {
                        matchingIds.add(songId);
                    }
                }

                filter.setSongIds(intersectIds(matchingIds, filter.getSongIds()));
            }

            private List<Integer> intersectIds(List<Integer> matchingIds, List<Integer> existingIds) {
                if (matchingIds.isEmpty()) {
                    return new ArrayList<>(List.of(-1));
                }

                if (existingIds != null && !existingIds.isEmpty()) {
                    matchingIds.retainAll(existingIds);
                    if (matchingIds.isEmpty()) {
                        return new ArrayList<>(List.of(-1));
                    }
                }

                return matchingIds;
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
    
    // Get filtered chart data for gender breakdown (using ChartFilterDTO)
    public Map<String, Object> getFilteredChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getFilteredChartData(filter);
    }
    
    // Get General tab chart data (5 pie charts: Artists, Albums, Songs, Plays, Listening Time)
    public Map<String, Object> getGeneralChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getGeneralChartData(filter);
    }
    
    // Get Genre tab chart data (5 bar charts grouped by genre)
    public Map<String, Object> getGenreChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getGenreChartData(filter);
    }
    
    // Get Subgenre tab chart data (5 bar charts grouped by subgenre)
    public Map<String, Object> getSubgenreChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getSubgenreChartData(filter);
    }
    
    // Get Ethnicity tab chart data (5 bar charts grouped by ethnicity)
    public Map<String, Object> getEthnicityChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getEthnicityChartData(filter);
    }
    
    // Get Language tab chart data (5 bar charts grouped by language)
    public Map<String, Object> getLanguageChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getLanguageChartData(filter);
    }
    
    // Get Country tab chart data (5 bar charts grouped by country)
    public Map<String, Object> getCountryChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getCountryChartData(filter);
    }
    
    // Get Release Year tab chart data (4 bar charts grouped by release year - no artists)
    public Map<String, Object> getReleaseYearChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getReleaseYearChartData(filter);
    }
    
    // Get Listen Year tab chart data (5 bar charts grouped by year listened)
    public Map<String, Object> getListenYearChartData(ChartFilterDTO filter) {
        applyItunesFilter(filter);
        return songRepository.getListenYearChartData(filter);
    }
    
    // Get plays for a song with pagination
    public List<PlayDTO> getPlaysForSong(int songId, int page, int pageSize) {
        int offset = page * pageSize;
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = """
            SELECT 
                p.id,
                p.play_date,
                s.name as song_name,
                s.id as song_id,
                a.name as album_name,
                a.id as album_id,
                ar.name as artist_name,
                ar.id as artist_id,
                p.account
            FROM Play p
            INNER JOIN Song s ON p.song_id = s.id
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album a ON s.album_id = a.id
            WHERE s.id IN (%s)
            ORDER BY p.play_date DESC
            LIMIT ? OFFSET ?
            """.formatted(placeholders(ids));
        
        Object[] params = new Object[ids.size() + 2];
        for (int i = 0; i < ids.size(); i++) params[i] = ids.get(i);
        params[ids.size()] = pageSize;
        params[ids.size() + 1] = offset;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlayDTO dto = new PlayDTO();
            dto.setId(rs.getInt("id"));
            dto.setPlayDate(rs.getString("play_date"));
            dto.setSongName(rs.getString("song_name"));
            dto.setSongId(rs.getInt("song_id"));
            dto.setAlbumName(rs.getString("album_name"));
            int albumId = rs.getInt("album_id");
            dto.setAlbumId(rs.wasNull() ? null : albumId);
            dto.setArtistName(rs.getString("artist_name"));
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setAccount(rs.getString("account"));
            return dto;
        }, params);
    }
    
    // Count total plays for a song
    public long countPlaysForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = "SELECT COUNT(*) FROM Play WHERE song_id IN (" + placeholders(ids) + ")";
        Long count = jdbcTemplate.queryForObject(sql, Long.class, ids.toArray());
        return count != null ? count : 0;
    }
    
    // Get plays by year for a song
    public List<PlaysByYearDTO> getPlaysByYearForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = """
            SELECT 
                strftime('%%Y', play_date) as year,
                COUNT(*) as play_count
            FROM Play
            WHERE song_id IN (%s) AND play_date IS NOT NULL
            GROUP BY strftime('%%Y', play_date)
            ORDER BY year ASC
            """.formatted(placeholders(ids));
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByYearDTO dto = new PlaysByYearDTO();
            dto.setYear(rs.getString("year"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, ids.toArray());
    }
    
    // Get plays by month for a song
    public List<PlaysByMonthDTO> getPlaysByMonthForSong(int songId) {
        List<Integer> ids = getEffectiveSongIdsForStats(songId);
        String sql = """
            SELECT 
                strftime('%%Y', play_date) as year,
                strftime('%%m', play_date) as month,
                COUNT(*) as play_count
            FROM Play
            WHERE song_id IN (%s) AND play_date IS NOT NULL
            GROUP BY strftime('%%Y', play_date), strftime('%%m', play_date)
            ORDER BY year ASC, month ASC
            """.formatted(placeholders(ids));
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            PlaysByMonthDTO dto = new PlaysByMonthDTO();
            dto.setYear(rs.getString("year"));
            dto.setMonth(rs.getString("month"));
            dto.setPlayCount(rs.getLong("play_count"));
            return dto;
        }, ids.toArray());
    }
    
    // ============================================
    // Featured Artists Methods
    // ============================================
    
    /**
     * Get all featured artists for a song
     */
    public List<FeaturedArtistDTO> getFeaturedArtistsForSong(int songId) {
        String sql = """
            SELECT sfa.artist_id, a.name as artist_name
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON sfa.artist_id = a.id
            WHERE sfa.song_id = ?
            ORDER BY a.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistDTO dto = new FeaturedArtistDTO();
            dto.setArtistId(rs.getInt("artist_id"));
            dto.setArtistName(rs.getString("artist_name"));
            return dto;
        }, songId);
    }
    
    /**
     * Search artists by name for the featured artists autocomplete
     */
    public List<FeaturedArtistDTO> searchArtists(String query, int limit) {
        String sql = """
            SELECT id, name
            FROM Artist
            WHERE LOWER(name) LIKE LOWER(?)
            ORDER BY name
            LIMIT ?
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistDTO dto = new FeaturedArtistDTO();
            dto.setArtistId(rs.getInt("id"));
            dto.setArtistName(rs.getString("name"));
            return dto;
        }, "%" + query + "%", limit);
    }
    
    /**
     * Save featured artists for a song (replaces all existing)
     */
    @Transactional
    public void saveFeaturedArtists(int songId, List<Integer> artistIds) {
        // First, delete all existing featured artists for this song
        jdbcTemplate.update("DELETE FROM SongFeaturedArtist WHERE song_id = ?", songId);
        
        // Then insert the new ones
        if (artistIds != null && !artistIds.isEmpty()) {
            String insertSql = "INSERT INTO SongFeaturedArtist (song_id, artist_id, creation_date) VALUES (?, ?, CURRENT_TIMESTAMP)";
            for (Integer artistId : artistIds) {
                jdbcTemplate.update(insertSql, songId, artistId);
            }
        }
    }
    
    /**
     * Get featured artist cards for a song (for the Featured Artists tab)
     * Returns full artist card data sorted alphabetically
     */
    public List<FeaturedArtistCardDTO> getFeaturedArtistCardsForSong(int songId) {
        String sql = """
            SELECT 
                a.id,
                a.name,
                a.gender_id,
                g.name as gender_name,
                a.ethnicity_id,
                e.name as ethnicity_name,
                a.genre_id,
                gr.name as genre_name,
                a.subgenre_id,
                sg.name as subgenre_name,
                a.language_id,
                l.name as language_name,
                a.country,
                (SELECT COUNT(*) FROM Song WHERE artist_id = a.id) as song_count,
                (SELECT COUNT(*) FROM Album WHERE artist_id = a.id) as album_count,
                CASE WHEN a.image IS NOT NULL THEN 1 ELSE 0 END as has_image,
                COALESCE(plays.play_count, 0) as play_count,
                COALESCE(plays.time_listened, 0) as time_listened,
                a.birth_date,
                a.death_date
            FROM SongFeaturedArtist sfa
            INNER JOIN Artist a ON sfa.artist_id = a.id
            LEFT JOIN Gender g ON a.gender_id = g.id
            LEFT JOIN Ethnicity e ON a.ethnicity_id = e.id
            LEFT JOIN Genre gr ON a.genre_id = gr.id
            LEFT JOIN SubGenre sg ON a.subgenre_id = sg.id
            LEFT JOIN Language l ON a.language_id = l.id
            LEFT JOIN (
                SELECT s.artist_id, COUNT(*) as play_count, 
                       SUM(COALESCE(s.length_seconds, 0)) as time_listened
                FROM Play p
                INNER JOIN Song s ON p.song_id = s.id
                GROUP BY s.artist_id
            ) plays ON a.id = plays.artist_id
            WHERE sfa.song_id = ?
            ORDER BY a.name
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            FeaturedArtistCardDTO dto = new FeaturedArtistCardDTO();
            dto.setId(rs.getInt("id"));
            dto.setName(rs.getString("name"));
            dto.setGenderId(rs.getObject("gender_id") != null ? rs.getInt("gender_id") : null);
            dto.setGenderName(rs.getString("gender_name"));
            dto.setEthnicityId(rs.getObject("ethnicity_id") != null ? rs.getInt("ethnicity_id") : null);
            dto.setEthnicityName(rs.getString("ethnicity_name"));
            dto.setGenreId(rs.getObject("genre_id") != null ? rs.getInt("genre_id") : null);
            dto.setGenreName(rs.getString("genre_name"));
            dto.setSubgenreId(rs.getObject("subgenre_id") != null ? rs.getInt("subgenre_id") : null);
            dto.setSubgenreName(rs.getString("subgenre_name"));
            dto.setLanguageId(rs.getObject("language_id") != null ? rs.getInt("language_id") : null);
            dto.setLanguageName(rs.getString("language_name"));
            dto.setCountry(rs.getString("country"));
            dto.setSongCount(rs.getInt("song_count"));
            dto.setAlbumCount(rs.getInt("album_count"));
            dto.setHasImage(rs.getInt("has_image") == 1);
            dto.setPlayCount(rs.getInt("play_count"));
            long timeListened = rs.getLong("time_listened");
            dto.setTimeListened(timeListened);
            dto.setTimeListenedFormatted(TimeFormatUtils.formatTime(timeListened));
            dto.setFeatureCount(1); // For songs, each featured artist only appears once
            String birthDateStr = rs.getString("birth_date");
            dto.setBirthDate(birthDateStr != null ? java.time.LocalDate.parse(birthDateStr) : null);
            String deathDateStr = rs.getString("death_date");
            dto.setDeathDate(deathDateStr != null ? java.time.LocalDate.parse(deathDateStr) : null);
            return dto;
        }, songId);
    }
    
    // Search songs by artist and song name for API
    public List<Map<String, Object>> searchSongs(String artistQuery, String songQuery, int limit) {
        StringBuilder sql = new StringBuilder(
            "SELECT s.id, s.name, a.id as artist_id, a.name as artist_name, al.name as album_name, " +
            "CASE WHEN s.single_cover IS NOT NULL OR EXISTS (SELECT 1 FROM SongImage WHERE song_id = s.id) THEN 1 ELSE 0 END as has_image " +
            "FROM Song s " +
            "JOIN Artist a ON s.artist_id = a.id " +
            "LEFT JOIN Album al ON s.album_id = al.id " +
            "WHERE 1=1 "
        );
        
        List<Object> params = new ArrayList<>();
        
        // Use normalized search (strip accents and special chars) and SQL normalization
        boolean hasArtist = artistQuery != null && !artistQuery.trim().isEmpty();
        boolean hasSong = songQuery != null && !songQuery.trim().isEmpty();
        String normalizedArtist = hasArtist ? library.util.StringNormalizer.normalizeForSearch(artistQuery) : null;
        String normalizedSong = hasSong ? library.util.StringNormalizer.normalizeForSearch(songQuery) : null;

        if (hasArtist && hasSong && normalizedArtist != null && normalizedArtist.equals(normalizedSong)) {
            // Unified search: match either artist OR song name
            sql.append("AND (" + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ? OR " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ?) ");
            params.add("%" + normalizedArtist + "%");
            params.add("%" + normalizedSong + "%");
        } else {
            // Separate filters: match both if provided
            if (hasArtist) {
                sql.append("AND " + library.util.StringNormalizer.sqlNormalizeColumn("a.name") + " LIKE ? ");
                params.add("%" + normalizedArtist + "%");
            }

            if (hasSong) {
                sql.append("AND " + library.util.StringNormalizer.sqlNormalizeColumn("s.name") + " LIKE ? ");
                params.add("%" + normalizedSong + "%");
            }
        }
        
        sql.append("ORDER BY a.name, s.name ");
        
        // Only apply limit if > 0
        if (limit > 0) {
            sql.append("LIMIT ?");
            params.add(limit);
        }
        
        return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> {
            Map<String, Object> song = new java.util.HashMap<>();
            song.put("id", rs.getInt("id"));
            song.put("name", rs.getString("name"));
            song.put("artistId", rs.getInt("artist_id"));
            song.put("artistName", rs.getString("artist_name"));
            song.put("albumName", rs.getString("album_name"));
            song.put("hasImage", rs.getInt("has_image") == 1);
            return song;
        }, params.toArray());
    }
    
    // ============= RANKING METHODS =============
    
    /**
     * Get all rankings for a song in a single query (optimized)
     * Returns a Map with keys: "gender", "genre", "subgenre", "ethnicity", "language", "country"
     */
    public java.util.Map<String, Integer> getAllSongRankings(int songId) {
        String sql = """
            WITH song_play_counts AS (
                SELECT s.id, 
                       ar.gender_id,
                       COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) as effective_genre_id,
                       COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) as effective_subgenre_id,
                       ar.ethnicity_id,
                       COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) as effective_language_id,
                       ar.country,
                       COALESCE(COUNT(p.id), 0) as play_count,
                       MIN(p.play_date) as first_play
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                GROUP BY s.id, ar.gender_id, effective_genre_id, effective_subgenre_id, 
                         ar.ethnicity_id, effective_language_id, ar.country
            ),
            ranked_songs AS (
                SELECT id,
                       gender_id,
                       effective_genre_id,
                       effective_subgenre_id,
                       ethnicity_id,
                       effective_language_id,
                       country,
                       play_count,
                       first_play,
                       CASE WHEN gender_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY gender_id ORDER BY play_count DESC, first_play ASC) 
                            END as gender_rank,
                       CASE WHEN effective_genre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_genre_id ORDER BY play_count DESC, first_play ASC) 
                            END as genre_rank,
                       CASE WHEN effective_subgenre_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_subgenre_id ORDER BY play_count DESC, first_play ASC) 
                            END as subgenre_rank,
                       CASE WHEN ethnicity_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY ethnicity_id ORDER BY play_count DESC, first_play ASC) 
                            END as ethnicity_rank,
                       CASE WHEN effective_language_id IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY effective_language_id ORDER BY play_count DESC, first_play ASC) 
                            END as language_rank,
                       CASE WHEN country IS NOT NULL 
                            THEN ROW_NUMBER() OVER (PARTITION BY country ORDER BY play_count DESC, first_play ASC) 
                            END as country_rank
                FROM song_play_counts
            )
            SELECT gender_rank, genre_rank, subgenre_rank, ethnicity_rank, language_rank, country_rank
            FROM ranked_songs
            WHERE id = ?
            """;
        
        java.util.Map<String, Integer> rankings = new java.util.HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            Integer genderRank = (Integer) rs.getObject("gender_rank");
            Integer genreRank = (Integer) rs.getObject("genre_rank");
            Integer subgenreRank = (Integer) rs.getObject("subgenre_rank");
            Integer ethnicityRank = (Integer) rs.getObject("ethnicity_rank");
            Integer languageRank = (Integer) rs.getObject("language_rank");
            Integer countryRank = (Integer) rs.getObject("country_rank");
            
            if (genderRank != null) rankings.put("gender", genderRank);
            if (genreRank != null) rankings.put("genre", genreRank);
            if (subgenreRank != null) rankings.put("subgenre", subgenreRank);
            if (ethnicityRank != null) rankings.put("ethnicity", ethnicityRank);
            if (languageRank != null) rankings.put("language", languageRank);
            if (countryRank != null) rankings.put("country", countryRank);
        }, songId);
        
        return rankings;
    }
    
    /**
     * Get song rank by genre (position within same genre based on play count, considering overrides)
     */
    public Integer getSongRankByGenre(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) 
                                          ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id)) IS NOT NULL
                GROUP BY s.id, COALESCE(s.override_genre_id, COALESCE(alb.override_genre_id, ar.genre_id))
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by subgenre (position within same subgenre based on play count, considering overrides)
     */
    public Integer getSongRankBySubgenre(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) 
                                          ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id)) IS NOT NULL
                GROUP BY s.id, COALESCE(s.override_subgenre_id, COALESCE(alb.override_subgenre_id, ar.subgenre_id))
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by ethnicity (position within same artist ethnicity based on play count)
     */
    public Integer getSongRankByEthnicity(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.ethnicity_id ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE ar.ethnicity_id IS NOT NULL
                GROUP BY s.id, ar.ethnicity_id
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by language (position within same language based on play count, considering overrides)
     */
    public Integer getSongRankByLanguage(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) 
                                          ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id)) IS NOT NULL
                GROUP BY s.id, COALESCE(s.override_language_id, COALESCE(alb.override_language_id, ar.language_id))
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song rank by country (position within same artist country based on play count)
     */
    public Integer getSongRankByCountry(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (PARTITION BY ar.country ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE ar.country IS NOT NULL
                GROUP BY s.id, ar.country
            ) ranked
            WHERE id = ?
            """;
        
        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Get song ranks by year (position within songs that had plays in each year)
     * Returns a map of year -> rank
     */
    public Map<Integer, Integer> getSongRanksByYear(int songId) {
        String sql = """
            SELECT year, rank FROM (
                SELECT s.id, 
                       strftime('%Y', p.play_date) as year,
                       ROW_NUMBER() OVER (PARTITION BY strftime('%Y', p.play_date) ORDER BY COUNT(p.id) DESC) as rank
                FROM Song s
                INNER JOIN Play p ON p.song_id = s.id
                GROUP BY s.id, strftime('%Y', p.play_date)
            ) ranked
            WHERE id = ?
            ORDER BY year
            """;
        
        java.util.Map<Integer, Integer> result = new java.util.LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            String yearStr = rs.getString("year");
            if (yearStr != null) {
                result.put(Integer.parseInt(yearStr), rs.getInt("rank"));
            }
        }, songId);
        return result;
    }

    /**
     * Get song's overall position among all songs by play count.
     */
    public Integer getSongOverallPosition(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                LEFT JOIN Play p ON p.song_id = s.id
                GROUP BY s.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song's position among songs released in the same year.
     * Uses the song's release date, or falls back to album's release date.
     */
    public Integer getSongRankByReleaseYear(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id,
                       strftime('%Y', COALESCE(s.release_date, alb.release_date)) as release_year,
                       ROW_NUMBER() OVER (
                           PARTITION BY strftime('%Y', COALESCE(s.release_date, alb.release_date)) 
                           ORDER BY COALESCE(COUNT(p.id), 0) DESC
                       ) as rank
                FROM Song s
                LEFT JOIN Album alb ON s.album_id = alb.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE COALESCE(s.release_date, alb.release_date) IS NOT NULL
                GROUP BY s.id, release_year
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get the release year for a song (from song's release_date or album's release_date).
     */
    public Integer getSongReleaseYear(int songId) {
        String sql = """
            SELECT strftime('%Y', COALESCE(s.release_date, alb.release_date)) as release_year
            FROM Song s
            LEFT JOIN Album alb ON s.album_id = alb.id
            WHERE s.id = ?
            """;

        try {
            String yearStr = jdbcTemplate.queryForObject(sql, String.class, songId);
            return yearStr != null ? Integer.parseInt(yearStr) : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song's position among all songs by the same artist.
     */
    public Integer getSongRankByArtist(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id,
                       s.artist_id,
                       ROW_NUMBER() OVER (PARTITION BY s.artist_id ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                LEFT JOIN Play p ON p.song_id = s.id
                GROUP BY s.id, s.artist_id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song's position among all songs in the same album.
     * Returns null if the song doesn't have an album.
     */
    public Integer getSongRankByAlbum(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id,
                       s.album_id,
                       ROW_NUMBER() OVER (PARTITION BY s.album_id ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE s.album_id IS NOT NULL
                GROUP BY s.id, s.album_id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get song rank for Spanish Rap (songs with effective genre=Rap and effective language=Spanish).
     */
    public Integer getSongSpanishRapRank(int songId) {
        String sql = """
            SELECT rank FROM (
                SELECT s.id, 
                       ROW_NUMBER() OVER (ORDER BY COALESCE(COUNT(p.id), 0) DESC) as rank
                FROM Song s
                INNER JOIN Artist ar ON s.artist_id = ar.id
                LEFT JOIN Album alb ON s.album_id = alb.id
                INNER JOIN Genre g ON COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) = g.id
                INNER JOIN Language l ON COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) = l.id
                LEFT JOIN Play p ON p.song_id = s.id
                WHERE g.name = 'Rap' AND l.name = 'Spanish'
                GROUP BY s.id
            ) ranked
            WHERE id = ?
            """;

        try {
            return jdbcTemplate.queryForObject(sql, Integer.class, songId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Check if a song is in the Spanish Rap category (effective genre=Rap AND effective language=Spanish)
     */
    public boolean isSongSpanishRap(int songId) {
        String sql = """
            SELECT COUNT(*) FROM Song s
            INNER JOIN Artist ar ON s.artist_id = ar.id
            LEFT JOIN Album alb ON s.album_id = alb.id
            INNER JOIN Genre g ON COALESCE(s.override_genre_id, alb.override_genre_id, ar.genre_id) = g.id
            INNER JOIN Language l ON COALESCE(s.override_language_id, alb.override_language_id, ar.language_id) = l.id
            WHERE s.id = ? AND g.name = 'Rap' AND l.name = 'Spanish'
            """;
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, songId);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public List<Map<String, Object>> getSongDetailsForIds(List<Integer> ids) {
        if (ids == null || ids.isEmpty()) return new ArrayList<>();
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        String sql = "SELECT id, name FROM Song WHERE id IN (" + placeholders + ")";
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> song = new HashMap<>();
            song.put("id", rs.getInt("id"));
            song.put("name", rs.getString("name"));
            return song;
        }, ids.toArray());
    }
}
