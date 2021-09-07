package library.dto;

import java.util.List;

public class TopGroupDTO {
	
	private String criteria;
	
	private List<TopCountDTO> listCounts;

	public String getCriteria() {
		return criteria;
	}

	public void setCriteria(String criteria) {
		this.criteria = criteria;
	}

	public List<TopCountDTO> getListCounts() {
		return listCounts;
	}

	public void setListCounts(List<TopCountDTO> listCounts) {
		this.listCounts = listCounts;
	}

	public TopGroupDTO(String criteria, List<TopCountDTO> listCounts) {
		super();
		this.criteria = criteria;
		this.listCounts = listCounts;
	}
	
}
