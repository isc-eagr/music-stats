package library.dto;

/**
 * Represents one "reign" of a #1 item (artist, song, or genre) in the top-played timeline.
 * Each entry shows who was #1, when they became #1, when they lost it, how many plays
 * they had at that point, and how long the reign lasted.
 */
public class TopPlayedTimelineEntryDTO {
    private int rank;               // sequential position (1, 2, 3, ...)
    private Integer itemId;         // artist_id, song_id, or genre_id
    private String itemName;        // artist name, "artist - song", or genre name
    private String secondaryName;   // artist name for songs, null for artists/genres
    private Integer genderId;       // gender id (for artist/song coloring)
    private String genderName;      // gender name
    private String startDate;       // date this item became #1 (dd/MM/yyyy)
    private String endDate;         // date this item lost #1 (dd/MM/yyyy or "Present")
    private int playsAtStart;       // cumulative plays when this item became #1
    private int playsAtEnd;         // cumulative plays when this item lost #1 (or current)
    private int daysAtNumberOne;    // duration of this individual reign in days
    private int totalDaysAllReigns; // sum of all days across all reigns of this item
    private int totalReignCount;    // total number of separate reigns for this item
    private int reignSequence;      // which reign number is this (1st, 2nd, 3rd, etc.)
    private boolean current;        // true if this is the current #1

    public int getRank() { return rank; }
    public void setRank(int rank) { this.rank = rank; }

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

    public String getStartDate() { return startDate; }
    public void setStartDate(String startDate) { this.startDate = startDate; }

    public String getEndDate() { return endDate; }
    public void setEndDate(String endDate) { this.endDate = endDate; }

    public int getPlaysAtStart() { return playsAtStart; }
    public void setPlaysAtStart(int playsAtStart) { this.playsAtStart = playsAtStart; }

    public int getPlaysAtEnd() { return playsAtEnd; }
    public void setPlaysAtEnd(int playsAtEnd) { this.playsAtEnd = playsAtEnd; }

    public int getDaysAtNumberOne() { return daysAtNumberOne; }
    public void setDaysAtNumberOne(int daysAtNumberOne) { this.daysAtNumberOne = daysAtNumberOne; }

    public int getTotalDaysAllReigns() { return totalDaysAllReigns; }
    public void setTotalDaysAllReigns(int totalDaysAllReigns) { this.totalDaysAllReigns = totalDaysAllReigns; }

    public int getTotalReignCount() { return totalReignCount; }
    public void setTotalReignCount(int totalReignCount) { this.totalReignCount = totalReignCount; }

    public int getReignSequence() { return reignSequence; }
    public void setReignSequence(int reignSequence) { this.reignSequence = reignSequence; }

    public boolean isCurrent() { return current; }
    public void setCurrent(boolean current) { this.current = current; }
}
