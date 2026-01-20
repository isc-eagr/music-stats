package library.dto;

/**
 * DTO for holding gender counts for list pages.
 */
public class GenderCountDTO {
    private long maleCount;
    private long femaleCount;
    private long otherCount;

    public GenderCountDTO() {
        this.maleCount = 0;
        this.femaleCount = 0;
        this.otherCount = 0;
    }

    public GenderCountDTO(long maleCount, long femaleCount, long otherCount) {
        this.maleCount = maleCount;
        this.femaleCount = femaleCount;
        this.otherCount = otherCount;
    }

    public long getMaleCount() {
        return maleCount;
    }

    public void setMaleCount(long maleCount) {
        this.maleCount = maleCount;
    }

    public long getFemaleCount() {
        return femaleCount;
    }

    public void setFemaleCount(long femaleCount) {
        this.femaleCount = femaleCount;
    }

    public long getOtherCount() {
        return otherCount;
    }

    public void setOtherCount(long otherCount) {
        this.otherCount = otherCount;
    }

    public long getTotal() {
        return maleCount + femaleCount + otherCount;
    }
}
