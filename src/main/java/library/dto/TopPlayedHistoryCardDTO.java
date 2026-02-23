package library.dto;

public class TopPlayedHistoryCardDTO {
    private Integer itemId;
    private String itemName;
    private String secondaryName;
    private Integer genderId;
    private String genderName;
    private int firstAppearanceRank;
    private int daysAtPos1;
    private int daysAtPos2;
    private int daysAtPos3;

    public Integer getItemId() {
        return itemId;
    }

    public void setItemId(Integer itemId) {
        this.itemId = itemId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public String getSecondaryName() {
        return secondaryName;
    }

    public void setSecondaryName(String secondaryName) {
        this.secondaryName = secondaryName;
    }

    public Integer getGenderId() {
        return genderId;
    }

    public void setGenderId(Integer genderId) {
        this.genderId = genderId;
    }

    public String getGenderName() {
        return genderName;
    }

    public void setGenderName(String genderName) {
        this.genderName = genderName;
    }

    public int getFirstAppearanceRank() {
        return firstAppearanceRank;
    }

    public void setFirstAppearanceRank(int firstAppearanceRank) {
        this.firstAppearanceRank = firstAppearanceRank;
    }

    public int getDaysAtPos1() {
        return daysAtPos1;
    }

    public void setDaysAtPos1(int daysAtPos1) {
        this.daysAtPos1 = daysAtPos1;
    }

    public int getDaysAtPos2() {
        return daysAtPos2;
    }

    public void setDaysAtPos2(int daysAtPos2) {
        this.daysAtPos2 = daysAtPos2;
    }

    public int getDaysAtPos3() {
        return daysAtPos3;
    }

    public void setDaysAtPos3(int daysAtPos3) {
        this.daysAtPos3 = daysAtPos3;
    }
}
