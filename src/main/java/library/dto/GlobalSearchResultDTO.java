package library.dto;

public class GlobalSearchResultDTO {

    private String type;
    private Integer id;
    private String name;
    private String detailUrl;
    private String imageUrl;
    private boolean hasImage;

    public GlobalSearchResultDTO() {
    }

    public GlobalSearchResultDTO(String type, Integer id, String name, String detailUrl, String imageUrl, boolean hasImage) {
        this.type = type;
        this.id = id;
        this.name = name;
        this.detailUrl = detailUrl;
        this.imageUrl = imageUrl;
        this.hasImage = hasImage;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDetailUrl() {
        return detailUrl;
    }

    public void setDetailUrl(String detailUrl) {
        this.detailUrl = detailUrl;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public boolean isHasImage() {
        return hasImage;
    }

    public void setHasImage(boolean hasImage) {
        this.hasImage = hasImage;
    }
}
