package library.entity;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

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
		album varchar(2000),
		account varchar(20)
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
	
	@Column (name="account")
	private String account;

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
		ZonedDateTime utcScrobbleDate = ZonedDateTime.parse(scrobbleDate+" UTC",DateTimeFormatter.ofPattern("dd MMM yyyy, HH:mm VV"));
		ZonedDateTime mexicanScrobbleDate = utcScrobbleDate.withZoneSameInstant(ZoneId.of("America/Mexico_City"));
		
		if(utcScrobbleDate.getYear()==1970)
			mexicanScrobbleDate = utcScrobbleDate.plusYears(40);
		
		
		this.scrobbleDate = mexicanScrobbleDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
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

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String toString() {
		return this.getArtist()+" - "+this.getSong()+" - "+this.getAlbum()+" - "
				+this.getScrobbleDate()+" - " + this.getId();
	}
	
}
