package library.dto;

import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Criterion<T> {
    
    public String name;
    public Function<T,String> groupingBy;

}
