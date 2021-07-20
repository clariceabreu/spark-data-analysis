import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.HashSet;

public class DataAnalysis {
    private final String PRINT_YELLOW = "\033[93m";
    private final String PRINT_BLUE = "\u001b[34;1m";
    private final String PRINT_RED = "\u001b[31m";
    private final String PRINT_GREEN = "\033[92m";
    private final String PRINT_COLOR_END = "\033[0m";
    private final String CLEAR_LINE = "\033[2K";

    private JavaRDD<String> data;

    public DataAnalysis(JavaRDD<String> data) {
        this.data = data;
    }

    public void mean(DatasetColumn column, HashSet<String> filter) {
        System.out.println();
        System.out.println("Calculating mean...");

        JavaRDD<Vector> columnData = getColumnData(column, filter);
        if (columnData.count() == 0) {
            System.out.println("Result: " + PRINT_GREEN + "0" + PRINT_COLOR_END);
            return;
        }

        MultivariateStatisticalSummary summary = Statistics.colStats(columnData.rdd());

        //Get mean and round to two decimals
        Double result = summary.mean().toArray()[0];
        result = Math.round(result * 100.0) / 100.0;

        System.out.println(CLEAR_LINE);
        System.out.println("Result: " + PRINT_GREEN + result + PRINT_COLOR_END);
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
