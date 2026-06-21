package library;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TemplateContractTest {

    private static final Path TEMPLATES = Path.of("src/main/resources/templates");

    @Test
    void catalogListPagesKeepCoreLayoutAnchorsInPlace() throws IOException {
        for (CatalogPage page : catalogPages()) {
            Document doc = parse(page.path());
            String html = read(page.path());

            assertThat(doc.select("#sortBySelect")).hasSize(1);
            assertThat(doc.select("#sortBySelect2")).hasSize(1);
            assertThat(doc.select("#sortBySelect3")).hasSize(1);
            assertThat(doc.select("#filterForm")).hasSize(1);
            assertThat(doc.select("#cardView")).hasSize(1);
            assertThat(doc.select("#tableView")).hasSize(1);
            assertThat(doc.select(page.tableSelector())).hasSize(1);
            assertThat(doc.select(page.cardSelector())).isNotEmpty();

            assertThat(doc.select(".gender-counters-container")).hasSize(1);
            assertThat(doc.select(".gender-male-counter")).hasSize(1);
            assertThat(doc.select(".gender-female-counter")).hasSize(1);
            assertThat(doc.select(".gender-other-counter")).hasSize(1);

            assertThat(html).contains("fragments/graphs-view :: graphs-view('" + page.entityType() + "')");
            assertThat(html).contains("switchView('card')");
            assertThat(html).contains("switchView('table')");
            assertThat(html).contains("switchView('graphs')");
        }
    }

    @Test
    void filteredListPagesExposeSavedFilterControls() throws IOException {
        for (String path : filteredListPages()) {
            Document doc = parse(path);
            String html = read(path);

            assertThat(html)
                    .describedAs(path)
                    .contains("fragments/saved-filters :: saved-filters");
            Element sortPanel = doc.selectFirst(".toolbar-sort-panel");
            List<Element> sortPanelChildren = sortPanel.children();
            int sortListIndex = -1;
            int savedFiltersIndex = -1;
            for (int i = 0; i < sortPanelChildren.size(); i++) {
                Element child = sortPanelChildren.get(i);
                if (child.hasClass("toolbar-sort-list")) {
                    sortListIndex = i;
                }
                if (child.attr("th:replace").contains("fragments/saved-filters :: saved-filters")) {
                    savedFiltersIndex = i;
                }
            }
            assertThat(savedFiltersIndex)
                    .describedAs(path + " keeps saved filters after sort controls")
                    .isGreaterThan(sortListIndex);
        }

        String fragment = read("fragments/saved-filters.html");
        assertThat(fragment)
                .contains("data-saved-filter-widget")
                .contains("saved-filter-select")
                .contains("saved-filter-save")
                .contains("saved-filter-apply")
                .contains("saved-filter-delete");

        String listUtils = Files.readString(Path.of("src/main/resources/static/js/list-utils.js"), StandardCharsets.UTF_8);
        assertThat(listUtils)
                .contains("/api/saved-filters")
                .contains("function initSavedFilters()")
                .contains("initSavedFilters();");
    }

    @Test
    void catalogTablesExposeStableColumnsForVisibleAndExtendedStats() throws IOException {
        assertColumns(
                "artists/list.html",
                Set.of(
                        "name", "random", "plays", "primaryPlays", "legacyPlays", "timeListened",
                        "firstListened", "lastListened", "daysListened", "weeksListened",
                        "monthsListened", "yearsListened", "age", "albumCount",
                        "avgLength", "avgPlays", "avgPlaysAlbum", "birthDate", "deathDate",
                        "songCount", "featuredOnCount", "featuredArtistCount", "soloSongCount",
                        "songsWithFeatCount", "genre", "subgenre", "ethnicity", "country", "language"
                )
        );

        assertColumns(
                "albums/list.html",
                Set.of(
                        "artistName", "name", "random", "plays", "primaryPlays", "legacyPlays", "length",
                        "timeListened", "releaseDate", "firstListened", "lastListened",
                        "daysListened", "weeksListened", "monthsListened", "yearsListened",
                        "lastFullListen", "ageAtRelease", "avgLength", "avgPlays",
                        "seasonalChartPeak", "songCount", "featuredArtistCount", "soloSongCount",
                        "songsWithFeatCount", "weeklyChartPeak", "weeklyChartWeeks",
                        "weeklyChartPeakWeeks", "yearlyChartPeak", "genre", "subgenre",
                        "ethnicity", "country", "language"
                )
        );

        assertColumns(
                "songs/list.html",
                Set.of(
                        "artistName", "albumName", "name", "random", "plays", "primaryPlays", "legacyPlays",
                        "length", "timeListened", "releaseDate", "firstListened", "lastListened",
                        "daysListened", "weeksListened", "monthsListened", "yearsListened",
                        "trackNumber", "ageAtRelease", "featuredArtistCount", "seasonalChartPeak",
                        "weeklyChartPeak", "weeklyChartWeeks", "weeklyChartPeakWeeks", "trlPeak",
                        "trlDays", "trlDaysAtPeak", "vatosCuntdownPeak", "vatosCuntdownDays",
                        "vatosCuntdownDaysAtPeak", "billboardPeak", "billboardWeeks",
                        "billboardWeeksAtPeak", "yearlyChartPeak", "genre", "subgenre",
                        "ethnicity", "country", "language"
                )
        );
    }

    @Test
    void catalogPrimarySortOptionsRemainUniqueAndCoverExpectedRegressionMetrics() throws IOException {
        Map<String, Set<String>> expectedSortOptions = Map.of(
                "artists/list.html", Set.of(
                        "name", "plays", "primary_plays", "legacy_plays", "time", "first_listened",
                        "last_listened", "days_listened", "weeks_listened", "months_listened",
                        "years_listened", "songs", "albums", "featured", "featured_artist_count",
                        "solo_songs", "songs_with_features", "genre", "subgenre", "ethnicity",
                        "country", "language", "itunes_presence", "random"
                ),
                "albums/list.html", Set.of(
                        "name", "artist", "plays", "primary_plays", "legacy_plays", "album_length",
                        "time", "release_date", "first_listened", "last_listened", "days_listened",
                        "weeks_listened", "months_listened", "years_listened", "last_full_listen",
                        "song_count", "featured_artist_count", "solo_songs", "songs_with_features",
                        "weekly_chart_peak", "weekly_chart_weeks", "weekly_chart_peak_weeks",
                        "yearly_chart_peak", "genre", "subgenre", "ethnicity", "country",
                        "language", "itunes_presence", "random"
                ),
                "songs/list.html", Set.of(
                        "name", "artist", "album", "plays", "primary_plays", "legacy_plays",
                        "length", "time", "release_date", "first_listened", "last_listened",
                        "days_listened", "weeks_listened", "months_listened", "years_listened",
                        "track_number", "age_at_release", "featured_artist_count",
                        "weekly_chart_peak", "weekly_chart_weeks", "weekly_chart_peak_weeks",
                        "trl_peak", "trl_days", "trl_days_at_peak", "vatos_cuntdown_peak",
                        "vatos_cuntdown_days", "vatos_cuntdown_days_at_peak", "billboard_peak",
                        "billboard_weeks", "billboard_weeks_at_peak", "yearly_chart_peak",
                        "genre", "subgenre", "ethnicity", "country", "language", "random"
                )
        );

        for (Map.Entry<String, Set<String>> entry : expectedSortOptions.entrySet()) {
            Document doc = parse(entry.getKey());
            List<String> values = doc.select("#sortBySelect option").eachAttr("value");

            assertThat(values).doesNotHaveDuplicates();
            assertThat(values).containsAll(entry.getValue());
        }
    }

    @Test
    void graphsFragmentKeepsEveryTabWiredToStatsAndChartContainers() throws IOException {
        Document doc = parse("fragments/graphs-view.html");
        List<String> tabs = List.of("general", "genre", "subgenre", "ethnicity", "language", "country", "releaseYear", "listenYear");

        assertThat(doc.select(".chart-sort-btn").eachAttr("data-metric"))
                .containsExactly("artists", "albums", "songs", "plays", "listeningTime");
        assertThat(doc.select(".charts-tab-btn").eachAttr("data-tab")).containsExactlyElementsOf(tabs);

        for (String tab : tabs) {
            assertThat(doc.select("#tab-" + tab)).hasSize(1);
            assertThat(doc.select("#" + tab + "CombinedChartContainer")).hasSize(1);
            if (!"general".equals(tab)) {
                assertThat(doc.select("#statsTablesSection-" + tab)).hasSize(1);
                assertThat(doc.select("#statsTablesContent-" + tab)).hasSize(1);
                assertThat(doc.select("#statsTablesRow-" + tab)).hasSize(1);
                assertThat(doc.select("#globalSort-" + tab)).hasSize(1);
            }
        }
    }

    @Test
    void templatesKeepMexicoStyleDateFormattingAndFlatpickrContracts() throws IOException {
        try (Stream<Path> paths = Files.walk(TEMPLATES)) {
            List<Path> htmlFiles = paths
                    .filter(path -> path.toString().endsWith(".html"))
                    .toList();

            for (Path path : htmlFiles) {
                String html = Files.readString(path, StandardCharsets.UTF_8);
                assertThat(html)
                        .describedAs(path.toString())
                        .doesNotContain("MM/dd/yyyy", "mm/dd/yyyy", "m/d/Y", "n/j/Y");
            }
        }

        String combinedCatalogHtml = read("artists/list.html") + read("albums/list.html") + read("songs/list.html");
        assertThat(combinedCatalogHtml).contains("flatpickr");
        assertThat(combinedCatalogHtml).contains("d/m/Y");
    }

    private static void assertColumns(String relativePath, Set<String> expectedColumns) throws IOException {
        Document doc = parse(relativePath);
        List<String> columns = doc.select("th[data-col]").eachAttr("data-col");
        List<String> sortColumns = doc.select("th[data-sort]").eachAttr("data-sort");

        assertThat(columns).doesNotHaveDuplicates();
        assertThat(sortColumns).doesNotHaveDuplicates();
        assertThat(columns).containsAll(expectedColumns);
        assertThat(sortColumns).containsAll(expectedColumns);
    }

    private static List<CatalogPage> catalogPages() {
        return List.of(
                new CatalogPage("artists/list.html", "artists", "#topArtistsTable", ".artist-card"),
                new CatalogPage("albums/list.html", "albums", "#topAlbumsTable", ".album-card"),
                new CatalogPage("songs/list.html", "songs", "#topSongsTable", ".song-card")
        );
    }

    private static List<String> filteredListPages() {
        return List.of(
                "artists/list.html",
                "albums/list.html",
                "songs/list.html",
                "timeframes/list.html",
                "countries/list.html",
                "ethnicities/list.html",
                "genders/list.html",
                "genres/list.html",
                "languages/list.html",
                "subgenres/list.html"
        );
    }

    private static Document parse(String relativePath) throws IOException {
        return Jsoup.parse(read(relativePath));
    }

    private static String read(String relativePath) throws IOException {
        return Files.readString(TEMPLATES.resolve(relativePath), StandardCharsets.UTF_8);
    }

    private record CatalogPage(String path, String entityType, String tableSelector, String cardSelector) {
    }
}
