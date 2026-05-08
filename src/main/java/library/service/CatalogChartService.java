package library.service;

import library.dto.ChartFilterDTO;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class CatalogChartService {

    private final SongService songService;

    public CatalogChartService(SongService songService) {
        this.songService = songService;
    }

    public Map<String, Object> getGeneralChartData(ChartFilterDTO filter) {
        return songService.getGeneralChartData(filter);
    }

    public Map<String, Object> getGenreChartData(ChartFilterDTO filter) {
        return songService.getGenreChartData(filter);
    }

    public Map<String, Object> getSubgenreChartData(ChartFilterDTO filter) {
        return songService.getSubgenreChartData(filter);
    }

    public Map<String, Object> getEthnicityChartData(ChartFilterDTO filter) {
        return songService.getEthnicityChartData(filter);
    }

    public Map<String, Object> getLanguageChartData(ChartFilterDTO filter) {
        return songService.getLanguageChartData(filter);
    }

    public Map<String, Object> getCountryChartData(ChartFilterDTO filter) {
        return songService.getCountryChartData(filter);
    }

    public Map<String, Object> getReleaseYearChartData(ChartFilterDTO filter) {
        return songService.getReleaseYearChartData(filter);
    }

    public Map<String, Object> getListenYearChartData(ChartFilterDTO filter) {
        return songService.getListenYearChartData(filter);
    }
}
