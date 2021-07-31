import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import java.util.HashSet;

public class DataAnalysis {
    private final String PRINT_YELLOW = "\033[93m";
    private final String PRINT_GREEN = "\033[92m";
    private final String PRINT_RED = "\u001b[31m";
    private final String PRINT_COLOR_END = "\033[0m";
    private final String CLEAR_LINE = "\033[2K";
    private final String RETURN_LINE = "\r";

    private JavaRDD<String> data;

    public DataAnalysis(JavaRDD<String> data) {
        this.data = data;
    }

    public Double mean(DatasetColumn column, HashSet<String> filter) {
        System.out.println();
        System.out.println("Calculating mean...");

        Mapper mapper = new Mapper();
        JavaRDD<Vector> columnData = mapper.getColumnData(data, column, filter);
        if (columnData.count() == 0) {
            System.out.print(PRINT_RED + "Dataset selected is empty" + PRINT_COLOR_END);
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

        Mapper mapper = new Mapper();
        JavaRDD<Vector> columnData = mapper.getColumnData(data, column, filter);
        if (columnData.count() == 0) {
            System.out.print(PRINT_RED + "Dataset selected is empty" + PRINT_COLOR_END);
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
        System.out.println();
        System.out.println("Calculating linear regression...");

        //Filter data
        Mapper mapper = new Mapper();
        JavaRDD<String> filteredData = data.filter(s -> mapper.filterData(s, filter));
        Double n = (double) filteredData.count();

        if (n == 0D) {
            System.out.print(PRINT_RED + "Dataset selected is empty" + PRINT_COLOR_END);
            return;
        }

        //Calculates summations
        Double[] summedColumns = filteredData.map(s -> mapper.mapColumns(s, columnToBasePrediction, columnToPredict))
                                              .reduce((x, y) -> mapper.sumColumns(x, y));
        Double sumXY = summedColumns[0];
        Double sumXX = summedColumns[1];
        Double sumX = summedColumns[2];
        Double sumY = summedColumns[3];

        //Calculates a numerator and denominator
        Double aNumerator = sumXY - (sumX * sumY)/n;
        Double aDenominator = sumXX - (sumX * sumX)/n;

        //Calculates a and b
        Double a = aNumerator / aDenominator;
        Double b = (sumY/n) - a * (sumX/n);
        Double roundedA =  Math.round(a * 100.0) / 100.0;
        Double roundedB =  Math.round(b * 100.0) / 100.0;

        //Predict value
        Double predicted = a * observedValue + b;
        Double roundedPredicted =  Math.round(predicted * 100.0) / 100.0;

        System.out.println();
        System.out.print("Regression function: " + PRINT_YELLOW +  "y = " + roundedA + "x + " + roundedB + PRINT_COLOR_END + RETURN_LINE);
        System.out.println("Predicted value: " + PRINT_GREEN + roundedPredicted + PRINT_COLOR_END);

        //Display chart with linear regression function and observed values
        System.out.println();
        System.out.println("Generating chart...");
        Chart chart = new Chart();
        chart.setxData(mapper.getColumnDataAsList(filteredData, columnToBasePrediction));
        chart.setyData(mapper.getColumnDataAsList(filteredData, columnToPredict));
        chart.setFunctionA(a);
        chart.setFunctionB(b);
        chart.setxLabel(columnToBasePrediction.toString());
        chart.setyLabel(columnToPredict.toString());
        chart.showChart(observedValue, predicted);
        System.out.println(PRINT_GREEN + "Chart opened in another window" + PRINT_COLOR_END);
    }
}

