package library.dto;

public class Filter {
	
    private String artist;
	
    private String song;
	
    private String album;
    
    private String sex;
    
    private String genre;
	
    private String race;
	
    private int year;
    
    private int playsMoreThan;
	
    private String language;
    
    private int page=1;
    
    private int pageSize=100000000;
    
    private boolean includeFeatures=false;
    
    private String sortField="count";
    
    private String sortDir="desc";
    
    private String filterMode="1";

	public String getArtist() {
		return artist;
	}

	public void setArtist(String artist) {
		this.artist = artist;
	}

	public String getSong() {
		return song;
	}

	public void setSong(String song) {
		this.song = song;
	}

	public String getAlbum() {
		return album;
	}

	public void setAlbum(String album) {
		this.album = album;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public String getRace() {
		return race;
	}

	public void setRace(String race) {
		this.race = race;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}
	
	public int getPlaysMoreThan() {
		return playsMoreThan;
	}

	public void setPlaysMoreThan(int playsMoreThan) {
		this.playsMoreThan = playsMoreThan;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public int getPage() {
		return page;
	}

	public void setPage(int page) {
		this.page = page;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public String getSortField() {
		return sortField;
	}

	public void setSortField(String sortField) {
		this.sortField = sortField;
	}

	public String getSortDir() {
		return sortDir;
	}

	public void setSortDir(String sortDir) {
		this.sortDir = sortDir;
	}

	public String getFilterMode() {
		return filterMode;
	}

	public void setFilterMode(String filterMode) {
		this.filterMode = filterMode;
	}

	public boolean isIncludeFeatures() {
		return includeFeatures;
	}

	public void setIncludeFeatures(boolean includeFeatures) {
		this.includeFeatures = includeFeatures;
	}
	
}
