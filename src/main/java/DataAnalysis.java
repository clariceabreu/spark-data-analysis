import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class DataAnalysis {
    private final String PRINT_YELLOW = "\033[93m";
    private final String PRINT_BLUE = "\u001b[34;1m";
    private final String PRINT_RED = "\u001b[31m";
    private final String PRINT_GREEN = "\033[92m";
    private final String PRINT_COLOR_END = "\033[0m";
    private final String CLEAR_LINE = "\033[2K";

    private final String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

    private JavaRDD<String> data;

    public DataAnalysis(JavaRDD<String> data) {
        this.data = data;
    }

    public Double mean(DatasetColumn column, HashSet<String> filter) {
        System.out.println();
        System.out.println("Calculating mean...");

        JavaRDD<Vector> columnData = getColumnData(column, filter);
        if (columnData.count() == 0) {
            System.out.println("Result: " + PRINT_GREEN + "0" + PRINT_COLOR_END);
            return null;
        }

        MultivariateStatisticalSummary summary = Statistics.colStats(columnData.rdd());

        //Get mean and round to two decimals
        Double result = summary.mean().toArray()[0];
        result = Math.round(result * 100.0) / 100.0;

        System.out.println(CLEAR_LINE);
        System.out.println("Result: " + PRINT_GREEN + result + PRINT_COLOR_END);
        return result;
    }

    public void standardDeviation(DatasetColumn column, HashSet<String> filter) {
        System.out.println();
        System.out.println("Calculating standard deviation...");

        JavaRDD<Vector> columnData = getColumnData(column, filter);
        if (columnData.count() == 0) {
            System.out.println("Result: " + PRINT_GREEN + "0" + PRINT_COLOR_END);
            return;
        }

        MultivariateStatisticalSummary summary = Statistics.colStats(columnData.rdd());

        //Get standard deviation (square root of the variance) and round to two decimals
        Double result = Math.sqrt(summary.variance().toArray()[0]);
        result = Math.round(result * 100.0) / 100.0;

        System.out.println(CLEAR_LINE);
        System.out.println("Result: " + PRINT_GREEN + result + PRINT_COLOR_END);
    }

    public void regression(DatasetColumn columnToBasePrediction, DatasetColumn columnToPredict, HashSet<String> filter){
       // List<Double> minMax = getMinAndMax(columnToBasePrediction);
        Double xMean = mean(columnToBasePrediction, filter);
        Double yMean = mean(columnToPredict, filter);
        Double upperSum = sumValues(columnToBasePrediction, columnToPredict, yMean);
        System.out.println(upperSum);
    }

    private Double sumValues(DatasetColumn column1, DatasetColumn column2, Double meanValue) {
        String result = data.map(s -> s)
                .reduce((x, y) -> {
            String[] columns = x.split(regexp);
            Double first = Double.parseDouble(columns[column1.index].replaceAll("\"", "").trim());
            Double second = Double.parseDouble(columns[column2.index].replaceAll("\"", "").trim());

            Double sum = first + (second - meanValue);

            columns[column1.index] = sum.toString();

            return String.join(",",columns);
        });

        Double value = Double.parseDouble(result.split(regexp)[column1.index].replaceAll("\"", "").trim());
        return  value;
    }

    private List<Double> getMinAndMax(DatasetColumn columnToBasePrediction) {
        String min = data.reduce((x, y) -> {
            String first = x.split(regexp)[columnToBasePrediction.index].replaceAll("\"", "").trim();
            String second = x.split(regexp)[columnToBasePrediction.index].replaceAll("\"", "").trim();

            if (Double.parseDouble(first) > Double.parseDouble(second)) {
                return y;
            } else {
                return x;
            }
        });

        String max = data.reduce((x, y) -> {
            String first = x.split(regexp)[columnToBasePrediction.index].replaceAll("\"", "").trim();
            String second = x.split(regexp)[columnToBasePrediction.index].replaceAll("\"", "").trim();

            if (Double.parseDouble(first) > Double.parseDouble(second)) {
                return  x;
            } else {
                return y;
            }
        });

        double maxValue = Double.parseDouble(max.split(regexp)[columnToBasePrediction.index].replaceAll("\"", "").trim());
        double minValue = Double.parseDouble(min.split(regexp)[columnToBasePrediction.index].replaceAll("\"", "").trim());

        List<Double> maxMin = new ArrayList<>();
        maxMin.add(minValue);
        maxMin.add(maxValue);
        return maxMin;
    }


    private JavaRDD<Vector> getColumnData(DatasetColumn column, HashSet<String> filter) {
        //Regex to split csv columns (prevent from splitting columns that have comma in its content into two columns)
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

        //Split line to get only data from a specific column, then removing quotation marks and white spaces
        JavaRDD<Vector> columnData = data
                .filter(s ->
                    filter.size() == 0 || filter.contains(s.split(regexp)[0].replaceAll("\"", "").trim())
                )
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
