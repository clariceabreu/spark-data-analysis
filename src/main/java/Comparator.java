import java.io.Serializable;

public class Comparator implements Serializable {

    public String max(String x, String y) {
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        String first = x.split(regexp)[8].replaceAll("\"", "").trim();
        String second = x.split(regexp)[8].replaceAll("\"", "").trim();

        if (Double.parseDouble(first) > Double.parseDouble(second)) {
            return y;
        } else {
            return x;
        }
    }

    public String min(String x, String y) {
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        String first = x.split(regexp)[8].replaceAll("\"", "").trim();
        String second = x.split(regexp)[8].replaceAll("\"", "").trim();

        if (Double.parseDouble(first) < Double.parseDouble(second)) {
            return y;
        } else {
            return x;
        }
    }

    public Double sum(Double x, Double y) {
        return x + y;
    }

    public Double mapToDouble(String x, DatasetColumn column1, DatasetColumn column2, Double meanValue){
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        String[] columns_1 = x.split(regexp);
        Double first1 = Double.parseDouble(columns_1[column1.index].replaceAll("\"", "").trim());
        Double second1 = Double.parseDouble(columns_1[column2.index].replaceAll("\"", "").trim());
        Double result = first1 * (second1 - meanValue);

        return result;
    }
}
