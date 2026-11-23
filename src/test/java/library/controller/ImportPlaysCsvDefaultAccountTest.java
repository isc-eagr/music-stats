package library.controller;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The Import Plays CSV upload defaults to the account the automation does not fetch. */
class ImportPlaysCsvDefaultAccountTest {

    @Test
    void picksTheFirstAccountThatIsNotAutomated() {
        assertThat(PlayController.csvDefaultAccount(List.of("vatito", "robertlover"), "vatito")).isEqualTo("robertlover");
        assertThat(PlayController.csvDefaultAccount(List.of("vatito", "robertlover"), " VATITO ")).isEqualTo("robertlover");
        assertThat(PlayController.csvDefaultAccount(List.of("vatito", "robertlover"), "robertlover")).isEqualTo("vatito");
    }

    @Test
    void fallsBackWhenEveryAccountIsAutomatedOrNoneIsSet() {
        assertThat(PlayController.csvDefaultAccount(List.of("vatito"), "vatito")).isEqualTo("vatito");
        assertThat(PlayController.csvDefaultAccount(List.of("vatito", "robertlover"), null)).isEqualTo("vatito");
        assertThat(PlayController.csvDefaultAccount(List.of(), "vatito")).isNull();
    }
}
