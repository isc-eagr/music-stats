package library.dto;

import java.util.List;

import library.entity.Song;

public class SaveAlbumDTO {
	
	private List<Song> songs;

	public List<Song> getSongs() {
		return songs;
	}

	public void setSongs(List<Song> songs) {
		this.songs = songs;
	}
	
}
