package library.dto;

public class CatalogWinningPeriodStatsDTO {
    private Integer winningDaysCount = 0;
    private Integer winningWeeksCount = 0;
    private Integer winningMonthsCount = 0;
    private Integer winningSeasonsCount = 0;
    private Integer winningYearsCount = 0;
    private Integer winningDecadesCount = 0;

    public Integer getWinningDaysCount() {
        return winningDaysCount != null ? winningDaysCount : 0;
    }

    public void setWinningDaysCount(Integer winningDaysCount) {
        this.winningDaysCount = winningDaysCount;
    }

    public Integer getWinningWeeksCount() {
        return winningWeeksCount != null ? winningWeeksCount : 0;
    }

    public void setWinningWeeksCount(Integer winningWeeksCount) {
        this.winningWeeksCount = winningWeeksCount;
    }

    public Integer getWinningMonthsCount() {
        return winningMonthsCount != null ? winningMonthsCount : 0;
    }

    public void setWinningMonthsCount(Integer winningMonthsCount) {
        this.winningMonthsCount = winningMonthsCount;
    }

    public Integer getWinningSeasonsCount() {
        return winningSeasonsCount != null ? winningSeasonsCount : 0;
    }

    public void setWinningSeasonsCount(Integer winningSeasonsCount) {
        this.winningSeasonsCount = winningSeasonsCount;
    }

    public Integer getWinningYearsCount() {
        return winningYearsCount != null ? winningYearsCount : 0;
    }

    public void setWinningYearsCount(Integer winningYearsCount) {
        this.winningYearsCount = winningYearsCount;
    }

    public Integer getWinningDecadesCount() {
        return winningDecadesCount != null ? winningDecadesCount : 0;
    }

    public void setWinningDecadesCount(Integer winningDecadesCount) {
        this.winningDecadesCount = winningDecadesCount;
    }

    public void resetWinningPeriodCounts() {
        winningDaysCount = 0;
        winningWeeksCount = 0;
        winningMonthsCount = 0;
        winningSeasonsCount = 0;
        winningYearsCount = 0;
        winningDecadesCount = 0;
    }
}
