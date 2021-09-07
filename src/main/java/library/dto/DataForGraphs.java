package library.dto;

public class DataForGraphs {
    
    String dataMale;
    String dataOthers;
    String labels;
    String metric;

    public String getDataMale() {
        return dataMale;
    }

    public void setDataMale(String dataMale) {
        this.dataMale = dataMale;
    }

    public String getDataOthers() {
        return dataOthers;
    }

    public void setDataOthers(String dataOthers) {
        this.dataOthers = dataOthers;
    }

    public String getLabels() {
        return labels;
    }

    public void setLabels(String labels) {
        this.labels = labels;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public DataForGraphs(String dataMale, String dataFemale, String labels, String metric) {
        this.dataMale = dataMale;
        this.dataOthers = dataFemale;
        this.labels = labels;
        this.metric = metric;
    }
    
}
