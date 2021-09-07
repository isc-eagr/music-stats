package library.dto;

import java.util.function.Function;

public class Criterion<T> {
    
    public String name;
    public Function<T,String> groupingBy;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Function<T, String> getGroupingBy() {
        return groupingBy;
    }

    public void setGroupingBy(Function<T, String> groupingBy) {
        this.groupingBy = groupingBy;
    }

    public Criterion(String name, Function<T, String> groupingBy) {
        this.name = name;
        this.groupingBy = groupingBy;
    }
    
    
    
}
