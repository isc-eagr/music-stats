package library.dto;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

public class Criterion<T> {
    
    public String name;
    public Function<T,String> groupingBy;
    public Comparator<Map.Entry<String, List<T>>> sortBy;
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
	public Comparator<Map.Entry<String, List<T>>> getSortBy() {
		return sortBy;
	}
	public void setSortBy(Comparator<Map.Entry<String, List<T>>> sortBy) {
		this.sortBy = sortBy;
	}
	public Criterion(String name, Function<T, String> groupingBy, Comparator<Entry<String, List<T>>> sortBy) {
		super();
		this.name = name;
		this.groupingBy = groupingBy;
		this.sortBy = sortBy;
	}

    
}
