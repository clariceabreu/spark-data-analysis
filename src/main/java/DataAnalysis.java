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

    private JavaRDD<String> data;
    Comparator comparator;

    public DataAnalysis(JavaRDD<String> data) {
        this.data = data;
        this.comparator = new Comparator();
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

    public void regression(DatasetColumn columnToBasePrediction, DatasetColumn columnToPredict, Double observedValue, HashSet<String> filter){
        Double xMean = mean(columnToBasePrediction, filter);
        Double yMean = mean(columnToPredict, filter);
        Double numerator = sumValues(columnToBasePrediction, columnToPredict, yMean);
        Double denominator = sumValues(columnToBasePrediction, columnToBasePrediction, xMean);
        Double b = numerator / denominator;
        Double a = yMean - (b * xMean);
        Double predicted = a + observedValue * b;
        Double roundedA =  Math.round(a * 100.0) / 100.0;
        Double roundedB =  Math.round(b * 100.0) / 100.0;

        System.out.println("Regression function: " + PRINT_YELLOW +  "y = " + roundedA + " + " + roundedB + "x" + PRINT_COLOR_END);
        System.out.println("Predicted value: " + PRINT_GREEN + predicted + PRINT_COLOR_END);
    }

    private Double sumValues(DatasetColumn column1, DatasetColumn column2, Double meanValue) {
        Comparator comparator = new Comparator();

        Double result = data.map(s -> comparator.mapToDouble(s, column1, column2, meanValue)).reduce((x, y) -> comparator.sum(x, y));

        return  result;
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

