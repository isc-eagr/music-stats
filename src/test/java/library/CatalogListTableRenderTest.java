package library;

import library.dto.CountryCardDTO;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Test;
import org.thymeleaf.context.Context;
import org.thymeleaf.context.IExpressionContext;
import org.thymeleaf.linkbuilder.StandardLinkBuilder;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Renders the secondary catalog table (Countries, Genres, ...) with the app's expression engine:
 * band row, pinned columns, medal ranks, male-share colors and the totals row must line up.
 */
class CatalogListTableRenderTest {

    @Test
    void metricTableRendersBandsMedalsLeanColorsAndTotals() {
        CountryCardDTO mexico = country(1, "Mexico", 900, 300, 2);
        CountryCardDTO colombia = country(2, "Colombia", 200, 600, 1);

        Document doc = render(List.of(mexico, colombia), "plays");
        Element table = doc.selectFirst("#catalogListTable");
        assertThat(table).isNotNull();

        int headerCells = table.select("thead tr:last-child th").size();
        int bandSpan = table.select("thead tr.cl-band-row th").stream()
                .mapToInt(th -> th.hasAttr("colspan") ? Integer.parseInt(th.attr("colspan")) : 1).sum();
        assertThat(bandSpan).describedAs("band row spans every header column").isEqualTo(headerCells);
        for (Element row : table.select("tbody tr")) {
            assertThat(row.select("td")).hasSize(headerCells);
        }
        assertThat(table.select("tfoot tr td")).hasSize(headerCells);

        assertThat(table.select("tbody tr").get(0).select("td").get(0).className()).contains("rank-1");
        assertThat(table.select("tbody tr").get(1).select("td").get(0).className()).contains("rank-2");
        assertThat(table.select("tbody tr").get(0).select("td.male-lean")).isNotEmpty();
        assertThat(table.select("tbody tr").get(1).select("td.female-lean")).isNotEmpty();
        assertThat(table.select("tbody a.cl-top-link.g-male")).isNotEmpty();
        assertThat(table.select("tbody a.cl-top-link.g-female")).isNotEmpty();
        assertThat(table.select("tfoot").text()).contains("Total of 2 countries");
    }

    @Test
    void emptyListKeepsOneFullWidthRowAndNoTotals() {
        Document doc = render(List.of(), "plays");
        Element table = doc.selectFirst("#catalogListTable");
        int headerCells = table.select("thead tr:last-child th").size();

        assertThat(table.select("tbody td")).hasSize(1);
        assertThat(Integer.parseInt(table.selectFirst("tbody td").attr("colspan"))).isEqualTo(headerCells);
        assertThat(table.select("tfoot")).isEmpty();
    }

    private static CountryCardDTO country(int id, String name, int malePlays, int femalePlays, int topGenderId) {
        CountryCardDTO dto = new CountryCardDTO();
        dto.setName(name);
        dto.setArtistCount(10);
        dto.setMaleArtistCount(malePlays > femalePlays ? 7 : 3);
        dto.setFemaleArtistCount(malePlays > femalePlays ? 3 : 7);
        dto.setPlayCount(malePlays + femalePlays);
        dto.setMalePlayCount(malePlays);
        dto.setFemalePlayCount(femalePlays);
        dto.setTimeListened(7200L);
        dto.setTopArtistId(id);
        dto.setTopArtistName("Top " + name);
        dto.setTopArtistGenderId(topGenderId);
        return dto;
    }

    private static Document render(List<CountryCardDTO> items, String sortBy) {
        ClassLoaderTemplateResolver resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        resolver.setTemplateMode(TemplateMode.HTML);
        resolver.setCharacterEncoding("UTF-8");

        SpringTemplateEngine engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);
        // No servlet here: resolve @{/...} links against an empty context path.
        engine.setLinkBuilder(new StandardLinkBuilder() {
            @Override
            protected String computeContextPath(IExpressionContext context, String base, Map<String, Object> parameters) {
                return "";
            }
        });

        Context context = new Context();
        // Fragment parameters left out of the selector are read from variables with the same names.
        context.setVariable("items", items);
        context.setVariable("emptyLabel", "countries");
        context.setVariable("showParent", false);
        context.setVariable("showMalePercentages", true);
        context.setVariable("showTop", true);
        context.setVariable("filterType", "country");
        context.setVariable("valueMode", "name");
        context.setVariable("nameSortKey", "name");
        context.setVariable("nameHeader", "Country");
        context.setVariable("sortBy", sortBy);
        context.setVariable("sortDir", "desc");
        context.setVariable("startIndex", 1);
        String html = engine.process("fragments/catalog-list-table", Set.of("metric-table"), context);
        return Jsoup.parse("<html><body>" + html + "</body></html>");
    }
}
