import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
    private static final String PRINT_GREEN = "\033[92m";
    private static final String PRINT_RED = "\u001b[31m";
    private static final String PRINT_BLUE = "\u001b[34;1m";
    private static final String PRINT_COLOR_END = "\033[0m";

    private static JavaSparkContext sparkContext;

    public static void main(String[] args) {
        config();

        enterAltTermBuffer();

        JavaRDD<String> data = loadData();

        leaveAltTermBuffer();
    }

    private static JavaRDD<String> loadData() {
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
                return loadData(years);
            }

            if (input.contains(":")) {
                String[] inputSplit = input.trim().split(":");
                int startYear = Integer.parseInt(inputSplit[0]);
                int endYear = Integer.parseInt(inputSplit[1]);
                return loadData(startYear, endYear);
            }

            int year = Integer.parseInt(input);
            return loadData(year, year);
        } catch (Exception e) {
            System.out.println(PRINT_RED + "Error: you have inserted an invalid year. Try again." + PRINT_COLOR_END);
            System.out.println();
            return loadData();
        }
    }

    private static JavaRDD<String> loadData(List<Integer> years) {
        JavaRDD<String> allData = sparkContext.emptyRDD();
        for (int year : years) {
            System.out.println();
            System.out.println("Loading data from year: " + year);
            File folder = new File("datasets/" + year);

            for (File file : folder.listFiles()) {
                JavaRDD<String> fileData = sparkContext.textFile(file.getPath());

                //Remove header
                String header = fileData.first();
                fileData = fileData.filter(line -> !line.equals(header));

                //Merge data
                allData = allData.union(fileData);
            }
        }

        return allData;
    }

    private static JavaRDD<String> loadData(int initialYear, int endYear) {
        List<Integer> years = new ArrayList<>();
        while (initialYear <= endYear) {
            years.add(initialYear);
            initialYear++;
        }

        return loadData(years);
    }

    private static void config() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster("local[3]").setAppName("NCDCdataAnalysis");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    private static void enterAltTermBuffer() {
        System.out.print("\033[?1049h\033[?25l");
    }

    private static void leaveAltTermBuffer() {
        System.out.println();
        System.out.println(PRINT_BLUE + "Press any key to exit" + PRINT_COLOR_END);
        Scanner s = new Scanner(System.in);
        s.nextLine();
        System.out.print("\033[?1049l");
        System.exit(0);
    }
}
