import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

public class Mapper implements Serializable {
    //Regex to split csv columns (prevent from splitting columns that have comma in its content into two columns)
    private final String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";;

    public boolean filterData(String s, HashSet<String> filter) {
        return s.length() > 0 && (filter.size() == 0 || filter.contains(s.split(regexp)[0].replaceAll("\"", "").trim()));
    }

    public Double[] mapColumns(String x, DatasetColumn column1, DatasetColumn column2) {
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        Double value1 = Double.parseDouble(x.split(regexp)[column1.index].replaceAll("\"", "").trim());
        Double value2 = Double.parseDouble(x.split(regexp)[column2.index].replaceAll("\"", "").trim());

        Double[] mappings = new Double[4];
        //x * y
        mappings[0] = value1 * value2;
        //x * x
        mappings[1] = value1 * value1;
        //x
        mappings[2] = value1;
        //y
        mappings[3] = value2;

        return mappings;
    }

    public Double[] sumColumns(Double[] line1, Double[] line2) {
        for (int i = 0; i < line1.length; i++) {
            line1[i] += line2[i];
        }

        return line1;
    }

    public List<Double> getColumnDataAsList(JavaRDD<String> data, DatasetColumn column) {
        //Split line to get only data from a specific column, then removing quotation marks and white spaces
        JavaRDD<Double> columnData = data.map(s -> {
            try {
                String newValue = s.split(regexp)[column.index].replaceAll("\"", "").trim();
                return Double.parseDouble(newValue);
            } catch (Exception e) {
                return 0D;
            }
        });

        return columnData.collect();
    }

    public JavaRDD<Vector> getColumnData(JavaRDD<String> data, DatasetColumn column, HashSet<String> filter) {
        //Split line to get only data from a specific column, then removing quotation marks and white spaces
        JavaRDD<Vector> columnData = data
                .filter(s -> this.filterData(s, filter))
                .map(s -> {
                    try {
                        String newValue = s.split(regexp)[column.index].replaceAll("\"", "").trim();
                        return Vectors.dense(Double.parseDouble(newValue));
                    } catch (Exception e) {
                        return Vectors.dense(0D);
                    }
                });

        return  columnData;
    }
}

