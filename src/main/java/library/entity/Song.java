package library.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table (name="song", schema="stats")
public class Song {
    
/*
 create table song (
id integer auto_increment primary key,
artist varchar(2000),
song varchar(2000),
album varchar(2000),
year integer,
language varchar(60),
genre varchar(60),
duration integer,
sex varchar(30),
cloud_status varchar(60),
source varchar(30),
created date,
updated date
); 
 */

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column (name="id")
	private int id;
	
	@Column (name="artist")
    private String artist;
	
	@Column (name="song")
    private String song;
	
	@Column (name="album")
    private String album;
	
	@Column (name="year")
    private int year;
	
	@Column (name="language")
    private String language;
	
	@Column (name="genre")
    private String genre;
	
	@Column (name="source")
	private String source;
	
	@Column (name="duration")
    private int duration;
	
	@Column (name="sex")
    private String sex;
	
	@Column (name="cloud_status")
    private String cloudStatus;
	
	@Column (name="created")
    private String created;
	
	@Column (name="updated")
    private String updated;

	public String toString() {
    	return this.artist+" - "+this.song+" - "+this.album+" - "+this.genre+" - "+" - "+this.sex;
    }

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
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
		return album == null?"":album;
	}

	public void setAlbum(String album) {
		this.album = album;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getCloudStatus() {
		return cloudStatus;
	}

	public void setCloudStatus(String cloudStatus) {
		this.cloudStatus = cloudStatus;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getUpdated() {
		return updated;
	}

	public void setUpdated(String updated) {
		this.updated = updated;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
	
}
