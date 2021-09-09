package library.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopGroupDTO {
	
	private String criteria;
	
	private List<TopCountDTO> listCounts;

}
