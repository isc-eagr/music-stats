package library.service;

import library.dto.SavedFilterDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SavedFilterServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void savesFiltersToFileAndReloadsThem() {
        Path storageFile = tempDir.resolve("saved-filters.properties");
        SavedFilterService service = new SavedFilterService(storageFile.toString());

        service.save(new SavedFilterDTO("/artists", "Male pop", "gender=1&genre=2&sortby=plays"));
        service.save(new SavedFilterDTO("/artists", "Female pop", "gender=2&genre=2&sortby=plays"));
        service.save(new SavedFilterDTO("/songs", "Long songs", "lengthMin=240&sortby=length"));

        SavedFilterService reloadedService = new SavedFilterService(storageFile.toString());

        assertThat(Files.exists(storageFile)).isTrue();
        assertThat(reloadedService.list("/artists"))
                .extracting(SavedFilterDTO::name)
                .containsExactly("Female pop", "Male pop");
        assertThat(reloadedService.list("/artists"))
                .extracting(SavedFilterDTO::query)
                .containsExactly("gender=2&genre=2&sortby=plays", "gender=1&genre=2&sortby=plays");
    }

    @Test
    void deletesOnlyTheNamedFilterForThePage() {
        Path storageFile = tempDir.resolve("saved-filters.properties");
        SavedFilterService service = new SavedFilterService(storageFile.toString());

        service.save(new SavedFilterDTO("/artists", "Favorites", "genre=1"));
        service.save(new SavedFilterDTO("/songs", "Favorites", "genre=1"));

        assertThat(service.delete("/artists", "Favorites")).isTrue();

        assertThat(service.list("/artists")).isEmpty();
        assertThat(service.list("/songs")).hasSize(1);
    }
}
