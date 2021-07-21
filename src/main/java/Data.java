import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Data {
    private static final String PRINT_GREEN = "\033[92m";
    private static final String PRINT_RED = "\u001b[31m";
    private static final String PRINT_COLOR_END = "\033[0m";

    public static JavaRDD<String> loadData(JavaSparkContext sparkContext) {
        System.out.println("Enter the years to load the data. Write the years separated by ',' or use ':' to specify a time range.");
        System.out.println("For example:");
        System.out.println(PRINT_GREEN + "2010, 2015, 2018" + PRINT_COLOR_END + " -> loads data from the specified years");
        System.out.println(PRINT_GREEN + "2010:2018" + PRINT_COLOR_END + " -> loads data from all years between 2010 and 2018 (including 2010 and 2018)");

        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();

        try {
            if (input.contains(",")) {
                String[] inputSplit = input.trim().split(",");
                List<Integer> years = new ArrayList<>();
                for (String year : inputSplit) {
                    years.add(Integer.parseInt(year));
                }
                return loadData(sparkContext, years);
            }

            if (input.contains(":")) {
                String[] inputSplit = input.trim().split(":");
                int startYear = Integer.parseInt(inputSplit[0]);
                int endYear = Integer.parseInt(inputSplit[1]);
                return loadData(sparkContext, startYear, endYear);
            }

            int year = Integer.parseInt(input);
            return loadData(sparkContext, year, year);
        } catch (Exception e) {
            System.out.println(PRINT_RED + "Error: you have inserted an invalid year. Try again." + PRINT_COLOR_END);
            System.out.println();
            return loadData(sparkContext);
        }
    }

    private static JavaRDD<String> loadData(JavaSparkContext sparkContext, List<Integer> years) {
        JavaRDD<String> allData = sparkContext.emptyRDD();
        for (int year : years) {
            System.out.println();
            System.out.println("Loading data from year " + year + "...");

            JavaRDD<String> yearData = sparkContext.textFile("datasets/" + year + "/*.csv");

            //Remove header
            String header = yearData.first();
            yearData = yearData.filter(line -> !line.equals(header));

            //Merge data
            allData = allData.union(yearData);
        }
        System.out.println(PRINT_GREEN + "All data successfully loaded!" + PRINT_COLOR_END);
        System.out.println();
        return allData;
    }

    private static JavaRDD<String> loadData(JavaSparkContext sparkContext, int initialYear, int endYear) {
        List<Integer> years = new ArrayList<>();
        while (initialYear <= endYear) {
            years.add(initialYear);
            initialYear++;
        }

        return loadData(sparkContext, years);
    }

    public static Dataset<Row> createDataset(SparkSession sparkSession, JavaRDD<String> dataRDD) {
        return sparkSession.createDataFrame(dataRDD, DataRow.class);
    }
}
