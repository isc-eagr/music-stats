package library.dto;

import java.util.List;

/**
 * DTO encapsulating all filter parameters for chart API endpoints.
 * Centralizes filter parameters to reduce method signature complexity.
 */
public class ChartFilterDTO {
    
    // Text search filters
    private String name;
    private List<Integer> artistIds;
    private List<Integer> albumIds;
    private List<Integer> songIds;
    
    // Account filter
    private List<String> accounts;
    private String accountMode;
    
    // Genre filter
    private List<Integer> genreIds;
    private String genreMode;
    
    // Subgenre filter
    private List<Integer> subgenreIds;
    private String subgenreMode;
    
    // Language filter
    private List<Integer> languageIds;
    private String languageMode;
    
    // Gender filter
    private List<Integer> genderIds;
    private String genderMode;
    
    // Ethnicity filter
    private List<Integer> ethnicityIds;
    private String ethnicityMode;
    
    // Country filter
    private List<String> countries;
    private String countryMode;
    
    // Release Date filter (entity-aware: artist, album, song)
    private String releaseDate;
    private String releaseDateFrom;
    private String releaseDateTo;
    private String releaseDateMode;
    private String releaseDateEntity; // "artist", "album", "song"
    
    // First Listened Date filter (entity-aware: artist, album, song)
    private String firstListenedDate;
    private String firstListenedDateFrom;
    private String firstListenedDateTo;
    private String firstListenedDateMode;
    private String firstListenedDateEntity; // "artist", "album", "song"
    
    // Last Listened Date filter (entity-aware: artist, album, song)
    private String lastListenedDate;
    private String lastListenedDateFrom;
    private String lastListenedDateTo;
    private String lastListenedDateMode;
    private String lastListenedDateEntity; // "artist", "album", "song"
    
    // Listened Date Range (for timeframe filtering - applies to scrobbles)
    private String listenedDateFrom;
    private String listenedDateTo;
    
    // Play Count filter (entity-aware: artist, album, song)
    private Integer playCountMin;
    private Integer playCountMax;
    private String playCountEntity; // "artist", "album", "song"
    
    // Boolean filters
    private String hasFeaturedArtists; // "true", "false", or null
    private String isBand; // "true", "false", or null
    private String isSingle; // "true", "false", or null
    
    // Include toggles for Top Artists aggregation
    private boolean includeGroups; // Include songs from artist's groups
    private boolean includeFeatured; // Include featured songs
    
    // Limit for top charts
    private Integer topLimit;

    // For General tab: which entity the topLimit applies to when calculating scrobble-based metrics
    // Valid values: "artist", "album", "song" (default)
    private String limitEntity;
    
    // Albums chart performance filters
    private Integer albumsWeeklyChartPeak;
    private Integer albumsWeeklyChartWeeks;
    private Integer albumsSeasonalChartPeak;
    private Integer albumsSeasonalChartSeasons;
    private Integer albumsYearlyChartPeak;
    private Integer albumsYearlyChartYears;
    
    // Songs chart performance filters
    private Integer songsWeeklyChartPeak;
    private Integer songsWeeklyChartWeeks;
    private Integer songsSeasonalChartPeak;
    private Integer songsSeasonalChartSeasons;
    private Integer songsYearlyChartPeak;
    private Integer songsYearlyChartYears;

    // Builder pattern for fluent API
    public static ChartFilterDTO builder() {
        return new ChartFilterDTO();
    }
    
    // Getters and setters
    public String getName() { return name; }
    public ChartFilterDTO setName(String name) { this.name = name; return this; }
    
    public List<Integer> getArtistIds() { return artistIds; }
    public ChartFilterDTO setArtistIds(List<Integer> artistIds) { this.artistIds = artistIds; return this; }
    
    public List<Integer> getAlbumIds() { return albumIds; }
    public ChartFilterDTO setAlbumIds(List<Integer> albumIds) { this.albumIds = albumIds; return this; }
    
    public List<Integer> getSongIds() { return songIds; }
    public ChartFilterDTO setSongIds(List<Integer> songIds) { this.songIds = songIds; return this; }
    
    public List<String> getAccounts() { return accounts; }
    public ChartFilterDTO setAccounts(List<String> accounts) { this.accounts = accounts; return this; }
    
    public String getAccountMode() { return accountMode; }
    public ChartFilterDTO setAccountMode(String accountMode) { this.accountMode = accountMode; return this; }
    
    public List<Integer> getGenreIds() { return genreIds; }
    public ChartFilterDTO setGenreIds(List<Integer> genreIds) { this.genreIds = genreIds; return this; }
    
    public String getGenreMode() { return genreMode; }
    public ChartFilterDTO setGenreMode(String genreMode) { this.genreMode = genreMode; return this; }
    
    public List<Integer> getSubgenreIds() { return subgenreIds; }
    public ChartFilterDTO setSubgenreIds(List<Integer> subgenreIds) { this.subgenreIds = subgenreIds; return this; }
    
    public String getSubgenreMode() { return subgenreMode; }
    public ChartFilterDTO setSubgenreMode(String subgenreMode) { this.subgenreMode = subgenreMode; return this; }
    
    public List<Integer> getLanguageIds() { return languageIds; }
    public ChartFilterDTO setLanguageIds(List<Integer> languageIds) { this.languageIds = languageIds; return this; }
    
    public String getLanguageMode() { return languageMode; }
    public ChartFilterDTO setLanguageMode(String languageMode) { this.languageMode = languageMode; return this; }
    
    public List<Integer> getGenderIds() { return genderIds; }
    public ChartFilterDTO setGenderIds(List<Integer> genderIds) { this.genderIds = genderIds; return this; }
    
    public String getGenderMode() { return genderMode; }
    public ChartFilterDTO setGenderMode(String genderMode) { this.genderMode = genderMode; return this; }
    
    public List<Integer> getEthnicityIds() { return ethnicityIds; }
    public ChartFilterDTO setEthnicityIds(List<Integer> ethnicityIds) { this.ethnicityIds = ethnicityIds; return this; }
    
    public String getEthnicityMode() { return ethnicityMode; }
    public ChartFilterDTO setEthnicityMode(String ethnicityMode) { this.ethnicityMode = ethnicityMode; return this; }
    
    public List<String> getCountries() { return countries; }
    public ChartFilterDTO setCountries(List<String> countries) { this.countries = countries; return this; }
    
    public String getCountryMode() { return countryMode; }
    public ChartFilterDTO setCountryMode(String countryMode) { this.countryMode = countryMode; return this; }
    
    public String getReleaseDate() { return releaseDate; }
    public ChartFilterDTO setReleaseDate(String releaseDate) { this.releaseDate = releaseDate; return this; }
    
    public String getReleaseDateFrom() { return releaseDateFrom; }
    public ChartFilterDTO setReleaseDateFrom(String releaseDateFrom) { this.releaseDateFrom = releaseDateFrom; return this; }
    
    public String getReleaseDateTo() { return releaseDateTo; }
    public ChartFilterDTO setReleaseDateTo(String releaseDateTo) { this.releaseDateTo = releaseDateTo; return this; }
    
    public String getReleaseDateMode() { return releaseDateMode; }
    public ChartFilterDTO setReleaseDateMode(String releaseDateMode) { this.releaseDateMode = releaseDateMode; return this; }
    
    public String getReleaseDateEntity() { return releaseDateEntity; }
    public ChartFilterDTO setReleaseDateEntity(String releaseDateEntity) { this.releaseDateEntity = releaseDateEntity; return this; }
    
    public String getFirstListenedDate() { return firstListenedDate; }
    public ChartFilterDTO setFirstListenedDate(String firstListenedDate) { this.firstListenedDate = firstListenedDate; return this; }
    
    public String getFirstListenedDateFrom() { return firstListenedDateFrom; }
    public ChartFilterDTO setFirstListenedDateFrom(String firstListenedDateFrom) { this.firstListenedDateFrom = firstListenedDateFrom; return this; }
    
    public String getFirstListenedDateTo() { return firstListenedDateTo; }
    public ChartFilterDTO setFirstListenedDateTo(String firstListenedDateTo) { this.firstListenedDateTo = firstListenedDateTo; return this; }
    
    public String getFirstListenedDateMode() { return firstListenedDateMode; }
    public ChartFilterDTO setFirstListenedDateMode(String firstListenedDateMode) { this.firstListenedDateMode = firstListenedDateMode; return this; }
    
    public String getFirstListenedDateEntity() { return firstListenedDateEntity; }
    public ChartFilterDTO setFirstListenedDateEntity(String firstListenedDateEntity) { this.firstListenedDateEntity = firstListenedDateEntity; return this; }
    
    public String getLastListenedDate() { return lastListenedDate; }
    public ChartFilterDTO setLastListenedDate(String lastListenedDate) { this.lastListenedDate = lastListenedDate; return this; }
    
    public String getLastListenedDateFrom() { return lastListenedDateFrom; }
    public ChartFilterDTO setLastListenedDateFrom(String lastListenedDateFrom) { this.lastListenedDateFrom = lastListenedDateFrom; return this; }
    
    public String getLastListenedDateTo() { return lastListenedDateTo; }
    public ChartFilterDTO setLastListenedDateTo(String lastListenedDateTo) { this.lastListenedDateTo = lastListenedDateTo; return this; }
    
    public String getLastListenedDateMode() { return lastListenedDateMode; }
    public ChartFilterDTO setLastListenedDateMode(String lastListenedDateMode) { this.lastListenedDateMode = lastListenedDateMode; return this; }
    
    public String getLastListenedDateEntity() { return lastListenedDateEntity; }
    public ChartFilterDTO setLastListenedDateEntity(String lastListenedDateEntity) { this.lastListenedDateEntity = lastListenedDateEntity; return this; }
    
    public String getListenedDateFrom() { return listenedDateFrom; }
    public ChartFilterDTO setListenedDateFrom(String listenedDateFrom) { this.listenedDateFrom = listenedDateFrom; return this; }
    
    public String getListenedDateTo() { return listenedDateTo; }
    public ChartFilterDTO setListenedDateTo(String listenedDateTo) { this.listenedDateTo = listenedDateTo; return this; }
    
    public Integer getPlayCountMin() { return playCountMin; }
    public ChartFilterDTO setPlayCountMin(Integer playCountMin) { this.playCountMin = playCountMin; return this; }
    
    public Integer getPlayCountMax() { return playCountMax; }
    public ChartFilterDTO setPlayCountMax(Integer playCountMax) { this.playCountMax = playCountMax; return this; }
    
    public String getPlayCountEntity() { return playCountEntity; }
    public ChartFilterDTO setPlayCountEntity(String playCountEntity) { this.playCountEntity = playCountEntity; return this; }
    
    public String getHasFeaturedArtists() { return hasFeaturedArtists; }
    public ChartFilterDTO setHasFeaturedArtists(String hasFeaturedArtists) { this.hasFeaturedArtists = hasFeaturedArtists; return this; }
    
    public String getIsBand() { return isBand; }
    public ChartFilterDTO setIsBand(String isBand) { this.isBand = isBand; return this; }
    
    public String getIsSingle() { return isSingle; }
    public ChartFilterDTO setIsSingle(String isSingle) { this.isSingle = isSingle; return this; }
    
    public boolean isIncludeGroups() { return includeGroups; }
    public ChartFilterDTO setIncludeGroups(boolean includeGroups) { this.includeGroups = includeGroups; return this; }
    
    public boolean isIncludeFeatured() { return includeFeatured; }
    public ChartFilterDTO setIncludeFeatured(boolean includeFeatured) { this.includeFeatured = includeFeatured; return this; }
    
    public Integer getTopLimit() { return topLimit; }
    public ChartFilterDTO setTopLimit(Integer topLimit) { this.topLimit = topLimit; return this; }

    public String getLimitEntity() { return limitEntity; }
    public ChartFilterDTO setLimitEntity(String limitEntity) { this.limitEntity = limitEntity; return this; }
    
    // Albums chart performance filter getters/setters
    public Integer getAlbumsWeeklyChartPeak() { return albumsWeeklyChartPeak; }
    public ChartFilterDTO setAlbumsWeeklyChartPeak(Integer albumsWeeklyChartPeak) { this.albumsWeeklyChartPeak = albumsWeeklyChartPeak; return this; }
    
    public Integer getAlbumsWeeklyChartWeeks() { return albumsWeeklyChartWeeks; }
    public ChartFilterDTO setAlbumsWeeklyChartWeeks(Integer albumsWeeklyChartWeeks) { this.albumsWeeklyChartWeeks = albumsWeeklyChartWeeks; return this; }
    
    public Integer getAlbumsSeasonalChartPeak() { return albumsSeasonalChartPeak; }
    public ChartFilterDTO setAlbumsSeasonalChartPeak(Integer albumsSeasonalChartPeak) { this.albumsSeasonalChartPeak = albumsSeasonalChartPeak; return this; }
    
    public Integer getAlbumsSeasonalChartSeasons() { return albumsSeasonalChartSeasons; }
    public ChartFilterDTO setAlbumsSeasonalChartSeasons(Integer albumsSeasonalChartSeasons) { this.albumsSeasonalChartSeasons = albumsSeasonalChartSeasons; return this; }
    
    public Integer getAlbumsYearlyChartPeak() { return albumsYearlyChartPeak; }
    public ChartFilterDTO setAlbumsYearlyChartPeak(Integer albumsYearlyChartPeak) { this.albumsYearlyChartPeak = albumsYearlyChartPeak; return this; }
    
    public Integer getAlbumsYearlyChartYears() { return albumsYearlyChartYears; }
    public ChartFilterDTO setAlbumsYearlyChartYears(Integer albumsYearlyChartYears) { this.albumsYearlyChartYears = albumsYearlyChartYears; return this; }
    
    // Songs chart performance filter getters/setters
    public Integer getSongsWeeklyChartPeak() { return songsWeeklyChartPeak; }
    public ChartFilterDTO setSongsWeeklyChartPeak(Integer songsWeeklyChartPeak) { this.songsWeeklyChartPeak = songsWeeklyChartPeak; return this; }
    
    public Integer getSongsWeeklyChartWeeks() { return songsWeeklyChartWeeks; }
    public ChartFilterDTO setSongsWeeklyChartWeeks(Integer songsWeeklyChartWeeks) { this.songsWeeklyChartWeeks = songsWeeklyChartWeeks; return this; }
    
    public Integer getSongsSeasonalChartPeak() { return songsSeasonalChartPeak; }
    public ChartFilterDTO setSongsSeasonalChartPeak(Integer songsSeasonalChartPeak) { this.songsSeasonalChartPeak = songsSeasonalChartPeak; return this; }
    
    public Integer getSongsSeasonalChartSeasons() { return songsSeasonalChartSeasons; }
    public ChartFilterDTO setSongsSeasonalChartSeasons(Integer songsSeasonalChartSeasons) { this.songsSeasonalChartSeasons = songsSeasonalChartSeasons; return this; }
    
    public Integer getSongsYearlyChartPeak() { return songsYearlyChartPeak; }
    public ChartFilterDTO setSongsYearlyChartPeak(Integer songsYearlyChartPeak) { this.songsYearlyChartPeak = songsYearlyChartPeak; return this; }
    
    public Integer getSongsYearlyChartYears() { return songsYearlyChartYears; }
    public ChartFilterDTO setSongsYearlyChartYears(Integer songsYearlyChartYears) { this.songsYearlyChartYears = songsYearlyChartYears; return this; }

    /**
     * Checks if any filter is active (for optimization - skip filtering if nothing is set)
     */
    public boolean hasAnyFilter() {
        return (name != null && !name.isEmpty()) ||
               (artistIds != null && !artistIds.isEmpty()) ||
               (albumIds != null && !albumIds.isEmpty()) ||
               (songIds != null && !songIds.isEmpty()) ||
               (accounts != null && !accounts.isEmpty()) ||
               (genreIds != null && !genreIds.isEmpty()) || genreMode != null ||
               (subgenreIds != null && !subgenreIds.isEmpty()) || subgenreMode != null ||
               (languageIds != null && !languageIds.isEmpty()) || languageMode != null ||
               (genderIds != null && !genderIds.isEmpty()) || genderMode != null ||
               (ethnicityIds != null && !ethnicityIds.isEmpty()) || ethnicityMode != null ||
               (countries != null && !countries.isEmpty()) || countryMode != null ||
               (releaseDate != null && !releaseDate.isEmpty()) ||
               (releaseDateFrom != null && !releaseDateFrom.isEmpty()) ||
               (releaseDateTo != null && !releaseDateTo.isEmpty()) ||
               (firstListenedDate != null && !firstListenedDate.isEmpty()) ||
               (firstListenedDateFrom != null && !firstListenedDateFrom.isEmpty()) ||
               (firstListenedDateTo != null && !firstListenedDateTo.isEmpty()) ||
               (lastListenedDate != null && !lastListenedDate.isEmpty()) ||
               (lastListenedDateFrom != null && !lastListenedDateFrom.isEmpty()) ||
               (lastListenedDateTo != null && !lastListenedDateTo.isEmpty()) ||
               (listenedDateFrom != null && !listenedDateFrom.isEmpty()) ||
               (listenedDateTo != null && !listenedDateTo.isEmpty()) ||
               playCountMin != null || playCountMax != null ||
               hasFeaturedArtists != null || isBand != null || isSingle != null ||
               // Albums chart filters
               albumsWeeklyChartPeak != null || albumsWeeklyChartWeeks != null ||
               albumsSeasonalChartPeak != null || albumsSeasonalChartSeasons != null ||
               albumsYearlyChartPeak != null || albumsYearlyChartYears != null ||
               // Songs chart filters
               songsWeeklyChartPeak != null || songsWeeklyChartWeeks != null ||
               songsSeasonalChartPeak != null || songsSeasonalChartSeasons != null ||
               songsYearlyChartPeak != null || songsYearlyChartYears != null;
    }
}
