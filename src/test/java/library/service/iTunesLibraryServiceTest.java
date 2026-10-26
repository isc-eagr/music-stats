package library.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;


import static org.assertj.core.api.Assertions.assertThat;

class iTunesLibraryServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void accentedSongUsesSameIdentityAsPresenceChip() throws Exception {
        Path xml = writeLibrary(track(1, "&#xE8;m&#243;", "Dei V", "LOS FLAVORZ", 123000));
        iTunesLibraryService service = new iTunesLibraryService();
        iTunesLibraryService library = new iTunesLibraryService() {
            @Override
            public String getDefaultLibraryPath() {
                return xml.toString();
            }

            @Override
            public boolean libraryExists() {
                return true;
            }
        };
        ItunesService presence = new ItunesService(null, library);

        for (String name : new String[] {"e\u0300mo\u0301", "\u00e8m\u00f3", "EMO"}) {
            var data = service.findTrackData(xml.toString(), name, "dei v", "los flavorz");
            assertThat(data).isNotNull();
            assertThat(data.matchType).isEqualTo("exact");
            assertThat(data.songName).isEqualTo("\u00e8m\u00f3");
            assertThat(presence.songExistsInItunes("dei v", "los flavorz", name)).isTrue();
            assertThat(service.findMatch(service.loadLibrary(xml.toString()), name, "dei v", "los flavorz"))
                    .isNotNull();
        }
    }

    @Test
    void exactAlbumMatchWinsOverEarlierPartialMatch() throws Exception {
        Path xml = writeLibrary(
                track(1, "Cuchi Cuchi", "Dei V", "Other album", 111000)
                + track(2, "Cuchi Cuchi", "Dei V", "LOS FLAVORZ", 222000));
        var data = new iTunesLibraryService().findTrackData(
                xml.toString(), "Cuchi Cuchi", "Dei V", "LOS FLAVORZ");

        assertThat(data).isNotNull();
        assertThat(data.matchType).isEqualTo("exact");
        assertThat(data.lengthSeconds).isEqualTo(222);
    }

    @Test
    void punctuationAndWhitespaceUsePresenceRules() throws Exception {
        Path xml = writeLibrary(track(1, "Cuchi Cuchi!", "Dei V", "LOS FLAVORZ", 123000));
        var data = new iTunesLibraryService().findTrackData(
                xml.toString(), " cuchi   cuchi ", "DEI V", "los flavorz");

        assertThat(data).isNotNull();
        assertThat(data.matchType).isEqualTo("exact");
    }

    @Test
    void differentAlbumRemainsPartialAndDifferentEditionDoesNotMatch() throws Exception {
        Path xml = writeLibrary(track(1, "Cuchi Cuchi", "Dei V", "LOS FLAVORZ", 123000));
        iTunesLibraryService service = new iTunesLibraryService();
        var data = service.findTrackData(xml.toString(), "Cuchi Cuchi", "Dei V", "Other album");

        assertThat(data).isNotNull();
        assertThat(data.matchType).isEqualTo("partial");
        assertThat(service.findTrackData(xml.toString(), "Cuchi Cuchi (Remix)", "Dei V", "LOS FLAVORZ"))
                .isNull();
        assertThat(service.findTrackData(xml.toString(), "Cuchi Cuchi", "Other artist", "LOS FLAVORZ"))
                .isNull();
        assertThat(service.findTrackData(xml.toString(), "", "Dei V", "LOS FLAVORZ")).isNull();
        assertThat(service.findMatch(Map.of(), "Cuchi Cuchi", "Dei V", "LOS FLAVORZ")).isNull();
    }

    private Path writeLibrary(String tracks) throws Exception {
        Path xml = tempDir.resolve("Library.xml");
        Files.writeString(xml, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<plist><dict><key>Tracks</key><dict>" + tracks + "</dict></dict></plist>");
        return xml;
    }

    private String track(int id, String name, String artist, String album, int milliseconds) {
        return "<key>" + id + "</key><dict><key>Persistent ID</key><string>TEST" + id
                + "</string><key>Name</key><string>" + name
                + "</string><key>Artist</key><string>" + artist
                + "</string><key>Album</key><string>" + album
                + "</string><key>Total Time</key><integer>" + milliseconds + "</integer></dict>";
    }

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
