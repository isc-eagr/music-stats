package library.dto;

import java.util.List;

public class SongMilestonesDTO {
	
	private String artist;
	private String album;
	private String song;
	private List<MilestoneDTO> milestones;
	
	public String getArtist() {
		return artist;
	}
	public void setArtist(String artist) {
		this.artist = artist;
	}
	public String getAlbum() {
		return album;
	}
	public void setAlbum(String album) {
		this.album = album;
	}
	public String getSong() {
		return song;
	}
	public void setSong(String song) {
		this.song = song;
	}
	public List<MilestoneDTO> getMilestones() {
		return milestones;
	}
	public void setMilestones(List<MilestoneDTO> milestones) {
		this.milestones = milestones;
	}
	public SongMilestonesDTO(String artist, String album, String song, List<MilestoneDTO> milestones) {
		super();
		this.artist = artist;
		this.album = album;
		this.song = song;
		this.milestones = milestones;
	}
	public SongMilestonesDTO() {
		super();
	}
	
		
}
