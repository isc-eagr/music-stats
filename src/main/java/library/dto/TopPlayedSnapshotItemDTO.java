package library.dto;

/**
 * Represents one item (artist, song, or genre) within a top-3 snapshot.
 * Tracks cumulative days spent at each position across all snapshots.
 */
public class TopPlayedSnapshotItemDTO {
    private Integer itemId;
    private String itemName;
    private String secondaryName;   // artist name for songs
    private Integer genderId;
    private String genderName;
    private int position;           // 1, 2, or 3
    private int playsCount;         // cumulative plays at this snapshot
    private int playsWhenEntered;   // cumulative plays when they moved to this specific position

    // Cumulative days spent at each position across ALL snapshots
    private int daysAtPos1;
    private int daysAtPos2;
    private int daysAtPos3;

    /**
     * Movement compared to the previous snapshot:
     * "NEW" = item was not in the previous top-3 (show purple),
     * "CLIMBED" = item moved to a better (lower-number) position (show green),
     * "DROPPED" = item moved to a worse (higher-number) position (show red),
     * null = no change / first snapshot.
     */
    private String movement;

    public Integer getItemId() { return itemId; }
    public void setItemId(Integer itemId) { this.itemId = itemId; }

    public String getItemName() { return itemName; }
    public void setItemName(String itemName) { this.itemName = itemName; }

    public String getSecondaryName() { return secondaryName; }
    public void setSecondaryName(String secondaryName) { this.secondaryName = secondaryName; }

    public Integer getGenderId() { return genderId; }
    public void setGenderId(Integer genderId) { this.genderId = genderId; }

    public String getGenderName() { return genderName; }
    public void setGenderName(String genderName) { this.genderName = genderName; }

    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    public int getPlaysCount() { return playsCount; }
    public void setPlaysCount(int playsCount) { this.playsCount = playsCount; }

    public int getPlaysWhenEntered() { return playsWhenEntered; }
    public void setPlaysWhenEntered(int playsWhenEntered) { this.playsWhenEntered = playsWhenEntered; }

    public int getDaysAtPos1() { return daysAtPos1; }
    public void setDaysAtPos1(int daysAtPos1) { this.daysAtPos1 = daysAtPos1; }

    public int getDaysAtPos2() { return daysAtPos2; }
    public void setDaysAtPos2(int daysAtPos2) { this.daysAtPos2 = daysAtPos2; }

    public int getDaysAtPos3() { return daysAtPos3; }
    public void setDaysAtPos3(int daysAtPos3) { this.daysAtPos3 = daysAtPos3; }

    public String getMovement() { return movement; }
    public void setMovement(String movement) { this.movement = movement; }

    /** Total days ever in the top 3 */
    public int getTotalDaysInTop3() {
        return daysAtPos1 + daysAtPos2 + daysAtPos3;
    }
}
