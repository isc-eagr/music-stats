package library.repository;

import java.util.List;

public interface ArtistRepositoryCustom {
    
    List<Object[]> findArtistsWithStats(
            String name,
            List<Integer> genderIds,
            String genderMode,
            List<Integer> ethnicityIds,
            String ethnicityMode,
            List<Integer> genreIds,
            String genreMode,
            List<Integer> subgenreIds,
            String subgenreMode,
            List<Integer> languageIds,
            String languageMode,
            List<String> countries,
            String countryMode,
            String firstListenedDate,
            String firstListenedDateFrom,
            String firstListenedDateTo,
            String firstListenedDateMode,
            String lastListenedDate,
            String lastListenedDateFrom,
            String lastListenedDateTo,
            String lastListenedDateMode,
            String sortBy,
            String sortDir,
            int limit,
            int offset
    );
    
    Long countArtistsWithFilters(
            String name,
            List<Integer> genderIds,
            String genderMode,
            List<Integer> ethnicityIds,
            String ethnicityMode,
            List<Integer> genreIds,
            String genreMode,
            List<Integer> subgenreIds,
            String subgenreMode,
            List<Integer> languageIds,
            String languageMode,
            List<String> countries,
            String countryMode,
            String firstListenedDate,
            String firstListenedDateFrom,
            String firstListenedDateTo,
            String firstListenedDateMode,
            String lastListenedDate,
            String lastListenedDateFrom,
            String lastListenedDateTo,
            String lastListenedDateMode
    );
}
