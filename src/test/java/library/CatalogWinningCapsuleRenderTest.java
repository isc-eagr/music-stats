package library;

import library.dto.CatalogWinningPeriodStatsDTO;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Renders the catalog card "Won" capsule with the same Spring expression engine the app uses,
 * so the inline period list in the fragment is checked for real.
 */
class CatalogWinningCapsuleRenderTest {

    @Test
    void capsuleListsEveryPeriodDimsZerosAndLightsTheSortedOne() {
        CatalogWinningPeriodStatsDTO item = new CatalogWinningPeriodStatsDTO();
        item.setWinningDaysCount(1840);
        item.setWinningWeeksCount(410);
        item.setWinningMonthsCount(92);
        item.setWinningSeasonsCount(22);
        item.setWinningYearsCount(6);
        item.setWinningDecadesCount(0);

        Document doc = render(item, "winningweeks");

        assertThat(doc.select(".lb-wins .lb-wins-k").text()).isEqualTo("Won");
        assertThat(doc.select(".lb-wins-c").eachText())
                .containsExactly("1,840days", "410wks", "92mos", "22seas", "6yrs", "0dec");
        assertThat(doc.select(".lb-wins-c.on").eachText()).containsExactly("410wks");
        assertThat(doc.select(".lb-wins-c.zero").eachText()).containsExactly("0dec");
    }

    @Test
    void nothingLightsUpWhenThePageIsNotSortedByAPeriod() {
        Document doc = render(new CatalogWinningPeriodStatsDTO(), "plays");

        assertThat(doc.select(".lb-wins-c")).hasSize(6);
        assertThat(doc.select(".lb-wins-c.on")).isEmpty();
        assertThat(doc.select(".lb-wins-c.zero")).hasSize(6);
    }

    private static Document render(CatalogWinningPeriodStatsDTO item, String sortBy) {
        ClassLoaderTemplateResolver resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        resolver.setTemplateMode(TemplateMode.HTML);
        resolver.setCharacterEncoding("UTF-8");

        SpringTemplateEngine engine = new SpringTemplateEngine();
        engine.setTemplateResolver(resolver);

        Context context = new Context();
        // Fragment parameters left out of the selector are read from variables with the same names.
        context.setVariable("it", item);
        context.setVariable("sortBy", sortBy);
        String html = engine.process("fragments/catalog-row", Set.of("wins"), context);
        return Jsoup.parseBodyFragment(html);
    }
}
