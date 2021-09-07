package library.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.opencsv.bean.CsvBindByPosition;

@Entity
@Table (name="scrobble", schema="stats")
public class Scrobble {
	
	/*
	 	create table scrobble (
		id integer primary key AUTO_INCREMENT,
		song_id integer,
        lastfm_id integer,
		scrobble_date datetime,
		artist varchar(2000),
		song varchar(2000),
		album varchar(2000)
		);
	*/
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column (name="id")
	private int id;
	
	@Column (name="lastfm_id")
	@CsvBindByPosition(position = 0)
	private int lastfmId;
	
	@Column (name="scrobble_date")
	@CsvBindByPosition(position = 1)
	private String scrobbleDate;
	
	@Column (name="artist")
	@CsvBindByPosition(position = 2)
	private String artist;
	
	@Column (name="song")
	@CsvBindByPosition(position = 6)
	private String song;
	
	@Column (name="album")
	@CsvBindByPosition(position = 4)
	private String album;
	
	@Column (name="song_id")
	private int songId;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getScrobbleDate() {
		return scrobbleDate;
	}

	public void setScrobbleDate(String scrobbleDate) {
		String [] csvFields = scrobbleDate.split(" ");
		String year =  csvFields[2].substring(0,4);
		
		this.scrobbleDate = (year.equals("1970")?"2010":year)+"-"
		+convertMonthStringToNumber(csvFields[1])+"-"
		+csvFields[0]+" "
		+csvFields[3];
	}

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
	
	public int getLastfmId() {
		return lastfmId;
	}

	public void setLastfmId(int lastfmId) {
		this.lastfmId = lastfmId;
	}

	public int getSongId() {
		return songId;
	}

	public void setSongId(int songId) {
		this.songId = songId;
	}

	public String convertMonthStringToNumber(String monthString) {
		String numberMonth = "00";
		switch(monthString) {
		case "Jan": numberMonth = "01"; break;
		case "Feb": numberMonth = "02"; break;
		case "Mar": numberMonth = "03"; break;
		case "Apr": numberMonth = "04"; break;
		case "May": numberMonth = "05"; break;
		case "Jun": numberMonth = "06"; break;
		case "Jul": numberMonth = "07"; break;
		case "Aug": numberMonth = "08"; break;
		case "Sep": numberMonth = "09"; break;
		case "Oct": numberMonth = "10"; break;
		case "Nov": numberMonth = "11"; break;
		case "Dec": numberMonth = "12"; break;
		
		}
		return numberMonth;
	}
	
	public String toString() {
		return this.getArtist()+" - "+this.getSong()+" - "+this.getAlbum()+" - "
				+this.getScrobbleDate()+" - " + this.getId();
	}
	
}
