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

    public Double squareColumn(String x, DatasetColumn column) {
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

        Double value = Double.parseDouble(x.split(regexp)[column.index].replaceAll("\"", "").trim());

        return value * value;
    }

    public Double mapToDouble(String x, DatasetColumn column1, DatasetColumn column2, Double meanValue){
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        String[] columns_1 = x.split(regexp);
        Double first1 = Double.parseDouble(columns_1[column1.index].replaceAll("\"", "").trim());
        Double second1 = Double.parseDouble(columns_1[column2.index].replaceAll("\"", "").trim());
        Double result = first1 * (second1 - meanValue);

        return result;
    }

    public Double mapColumn(String x, DatasetColumn column){
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

        Double value = Double.parseDouble(x.split(regexp)[column.index].replaceAll("\"", "").trim());

        return value;
    }

    public Double multiplyColumns(String x, DatasetColumn column1, DatasetColumn column2) {
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        Double value1 = Double.parseDouble(x.split(regexp)[column1.index].replaceAll("\"", "").trim());
        Double value2 = Double.parseDouble(x.split(regexp)[column2.index].replaceAll("\"", "").trim());

        return value1 * value2;
    }
}
