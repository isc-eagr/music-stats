package library.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataForGraphs {
    
    String dataMale;
    String dataOthers;
    String labels;
    String metric;    
}
