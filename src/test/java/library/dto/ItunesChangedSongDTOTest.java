package library.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ItunesChangedSongDTOTest {

    @Test
    void caseOnlyDifferencesAreNotChanges() {
        assertFalse(ItunesChangedSongDTO.textChanged("Dirty harry", "Dirty Harry"));
        assertFalse(ItunesChangedSongDTO.textChanged("GORILLAZ", "Gorillaz"));
        assertFalse(ItunesChangedSongDTO.textChanged("Canción (Remix)", "CANCIÓN (REMIX)"));
        assertFalse(ItunesChangedSongDTO.textChanged(null, null));
    }

    @Test
    void parenthesesAndSpecialCharacterDifferencesAreChanges() {
        assertTrue(ItunesChangedSongDTO.textChanged("Dirty Harry", "Dirty Harry (feat. Bootie Brown)"));
        assertTrue(ItunesChangedSongDTO.textChanged("Song (Remix)", "Song (Radio Edit)"));
        assertTrue(ItunesChangedSongDTO.textChanged("Song (Remix)", "Song [Remix]"));
        assertTrue(ItunesChangedSongDTO.textChanged("José", "Jose"));
        assertTrue(ItunesChangedSongDTO.textChanged("Hello!", "Hello"));
        assertTrue(ItunesChangedSongDTO.textChanged("Rock & Roll", "Rock and Roll"));
        assertTrue(ItunesChangedSongDTO.textChanged("Don't", "Don’t"));
        assertTrue(ItunesChangedSongDTO.textChanged("Album", null));
    }

    @Test
    void rowFlagsIgnoreCaseButKeepRealChanges() {
        ItunesChangedSongDTO dto = new ItunesChangedSongDTO("PID",
            "gorillaz", null, "demon days", "Dirty Harry", "Pop",
            "Gorillaz", null, "Demon Days", "Dirty Harry (feat. Bootie Brown)", "POP",
            1, 2005, true, 1L, 230_000, 230_400);

        assertFalse(dto.isArtistChanged());
        assertFalse(dto.isAlbumArtistChanged());
        assertFalse(dto.isAlbumChanged());
        assertFalse(dto.isGenreChanged());
        assertFalse(dto.isLengthChanged());
        assertTrue(dto.isNameChanged());
    }

    @Test
    void spacingOnlyEditsAreReportedAndShownVisibly() {
        assertTrue(ItunesChangedSongDTO.textChanged("The Show Must Go  On", "The Show Must Go On"));
        assertTrue(ItunesChangedSongDTO.textChanged("Popstar ", "Popstar"));

        ItunesChangedSongDTO dto = new ItunesChangedSongDTO();
        assertEquals("The Show Must Go··On", dto.showSpaces("The Show Must Go  On"));
        assertEquals("The Show Must Go On", dto.showSpaces("The Show Must Go On"));
        assertEquals("·Popstar·", dto.showSpaces(" Popstar "));
        assertEquals("Go·On", dto.showSpaces("Go On"));
        assertEquals("Go·On", dto.showSpaces("Go\tOn"));
        assertNull(dto.showSpaces(null));
    }

    @Test
    void lengthChangesOfOneSecondOrMoreAreChanges() {
        assertFalse(ItunesChangedSongDTO.lengthChanged(311_072, 311_000));
        assertTrue(ItunesChangedSongDTO.lengthChanged(311_000, 312_000));
        assertTrue(ItunesChangedSongDTO.lengthChanged(null, 312_000));
    }
}
