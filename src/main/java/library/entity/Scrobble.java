package library.entity;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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
	private Integer songId;
	
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
		if (scrobbleDate == null) {
			this.scrobbleDate = null;
			return;
		}
		String trimmed = scrobbleDate.trim();
		// Try multiple formats safely. If parsing fails, store the original trimmed value.
		// Known original pattern used in legacy code: "dd MMM yyyy, HH:mm VV" (e.g., "21 Nov 2025, 12:00 UTC")
		String[] patterns = new String[] {
			"dd MMM yyyy, HH:mm VV",
			"dd MMM yyyy, HH:mm",
			"yyyy-MM-dd HH:mm",
			"dd/MM/yyyy HH:mm",
			"dd MMM yyyy HH:mm",
		};
		for (String pattern : patterns) {
			try {
				DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);
				// When pattern includes zone (VV), parse as ZonedDateTime; otherwise assume UTC then convert
				if (pattern.contains("VV")) {
					ZonedDateTime utcScrobbleDate = ZonedDateTime.parse(trimmed + (pattern.contains("VV") && !trimmed.endsWith("UTC") && !trimmed.matches(".*\\s[A-Z]{3,}$") ? " UTC" : ""), fmt);
					ZonedDateTime mexicanScrobbleDate = utcScrobbleDate.withZoneSameInstant(ZoneId.of("America/Mexico_City"));
					if(utcScrobbleDate.getYear()==1970)
						mexicanScrobbleDate = utcScrobbleDate.plusYears(40);
					this.scrobbleDate = mexicanScrobbleDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
					return;
				} else {
					// Parse without zone; assume input is UTC-like and convert
					try {
						ZonedDateTime utcScrobbleDate = ZonedDateTime.parse(trimmed + " UTC", DateTimeFormatter.ofPattern(pattern + " VV"));
						ZonedDateTime mexicanScrobbleDate = utcScrobbleDate.withZoneSameInstant(ZoneId.of("America/Mexico_City"));
						if(utcScrobbleDate.getYear()==1970)
							mexicanScrobbleDate = utcScrobbleDate.plusYears(40);
						this.scrobbleDate = mexicanScrobbleDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
						return;
					} catch (DateTimeParseException e) {
						// fallback: try to parse LocalDateTime style via the pattern, then interpret as UTC
						try {
							java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(trimmed, DateTimeFormatter.ofPattern(pattern));
							ZonedDateTime utcScrobbleDate = ldt.atZone(ZoneId.of("UTC"));
							ZonedDateTime mexicanScrobbleDate = utcScrobbleDate.withZoneSameInstant(ZoneId.of("America/Mexico_City"));
							if(utcScrobbleDate.getYear()==1970)
								mexicanScrobbleDate = utcScrobbleDate.plusYears(40);
							this.scrobbleDate = mexicanScrobbleDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
							return;
						} catch (Exception ex) {
							// continue to next pattern
						}
					}
				}
			} catch (DateTimeParseException e) {
				// try next pattern
			}
		}
		// If we couldn't parse with any pattern, just save the raw trimmed string (no exception thrown)
		this.scrobbleDate = trimmed;
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

	public Integer getSongId() {
		return songId;
	}

	public void setSongId(Integer songId) {
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