package library.dto;

public class RankingChipDTO {
    private String label;
    private Integer position;
    private String category;
    private String filterType;
    private Integer filterId;
    private String filterName;

    public RankingChipDTO(String label, Integer position, String category, String filterType, Integer filterId, String filterName) {
        this.label = label;
        this.position = position;
        this.category = category;
        this.filterType = filterType;
        this.filterId = filterId;
        this.filterName = filterName;
    }

    // Getters and Setters
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getFilterType() {
        return filterType;
    }

    public void setFilterType(String filterType) {
        this.filterType = filterType;
    }

    public Integer getFilterId() {
        return filterId;
    }

    public void setFilterId(Integer filterId) {
        this.filterId = filterId;
    }

    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }
}
