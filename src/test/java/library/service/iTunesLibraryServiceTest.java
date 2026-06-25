package library.service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class iTunesLibraryServiceTest {

    @Test
    void findTrackDataReturnsExactSongNameFromItunes() throws Exception {
        Path tempDir = Path.of("target", "test-itunes-library-service");
        Files.createDirectories(tempDir);
        Path libraryXml = tempDir.resolve("iTunes Music Library.xml");
        Files.writeString(libraryXml, """
                <?xml version="1.0" encoding="UTF-8"?>
                <plist version="1.0">
                <dict>
                  <key>Tracks</key>
                  <dict>
                    <key>101</key>
                    <dict>
                      <key>Name</key><string>Bidi Bidi Bom Bom</string>
                      <key>Artist</key><string>Selena</string>
                      <key>Album</key><string>Amor Prohibido</string>
                      <key>Release Date</key><date>1994-03-13T12:00:00Z</date>
                      <key>Total Time</key><integer>209000</integer>
                      <key>Track Number</key><integer>2</integer>
                    </dict>
                  </dict>
                </dict>
                </plist>
                """, StandardCharsets.UTF_8);

        iTunesLibraryService service = new iTunesLibraryService();
        iTunesLibraryService.iTunesTrackData data = service.findTrackData(
                libraryXml.toString(),
                "bidi bidi bom bom",
                "selena",
                "amor prohibido");

        assertThat(data).isNotNull();
        assertThat(data.songName).isEqualTo("Bidi Bidi Bom Bom");
        assertThat(data.releaseDate).isEqualTo("1994-03-13");
        assertThat(data.lengthSeconds).isEqualTo(209);
        assertThat(data.trackNumber).isEqualTo(2);
        assertThat(data.matchType).isEqualTo("exact");
    }
}
