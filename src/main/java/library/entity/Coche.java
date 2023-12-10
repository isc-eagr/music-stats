package library.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table (name="coche", schema="stats")
public class Coche {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column (name="coche_id")
	private int cocheId;
	
	@Column (name="nombre")
    private String nombre;
	
	@ManyToOne
	@JoinColumn(name="personaId")
	private Persona persona;

	public int getCocheId() {
		return cocheId;
	}

	public void setCocheId(int cocheId) {
		this.cocheId = cocheId;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public Persona getPersona() {
		return persona;
	}

	public void setPersona(Persona persona) {
		this.persona = persona;
	}
	
	

}
