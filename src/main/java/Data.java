import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Data {
    private static final String PRINT_GREEN = "\033[92m";
    private static final String PRINT_RED = "\u001b[31m";
    private static final String PRINT_COLOR_END = "\033[0m";

    private JavaSparkContext sparkContext;
    private SQLContext sqlContext;
    private String header;
    private List<Integer> years = new ArrayList<>();

    public Data(JavaSparkContext sparkContext, SQLContext sqlContext) {
        this.sparkContext = sparkContext;
        this.sqlContext = sqlContext;
    }

    public JavaRDD<String> loadData() {
        System.out.println("Enter the years to load the data. Write the years separated by ',' or use ':' to specify a time range.");
        System.out.println("For example:");
        System.out.println(PRINT_GREEN + "2010, 2015, 2018" + PRINT_COLOR_END + " -> loads data from the specified years");
        System.out.println(PRINT_GREEN + "2010:2018" + PRINT_COLOR_END + " -> loads data from all years between 2010 and 2018 (including 2010 and 2018)");

        years = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();

        try {
            if (input.contains(",")) {
                String[] inputSplit = input.trim().split(",");
                years = new ArrayList<>();
                for (String year : inputSplit) {
                    years.add(Integer.parseInt(year));
                }
                return loadDataFromYears();
            }

            if (input.contains(":")) {
                String[] inputSplit = input.trim().split(":");
                int startYear = Integer.parseInt(inputSplit[0]);
                int endYear = Integer.parseInt(inputSplit[1]);

                while (startYear <= endYear) {
                    years.add(startYear);
                    startYear++;
                }

                return loadDataFromYears();
            }

            int year = Integer.parseInt(input);
            years.add(year);
            return loadDataFromYears();
        } catch (Exception e) {
            System.out.println(PRINT_RED + "Error: you have inserted an invalid year. Try again." + PRINT_COLOR_END);
            System.out.println();
            return loadData();
        }
    }

    private JavaRDD<String> loadDataFromYears() {
        JavaRDD<String> allData = sparkContext.emptyRDD();
        for (int year : years) {
            System.out.println();
            System.out.println("Loading data from year " + year + "...");

            JavaRDD<String> yearData = sparkContext.textFile("datasets/" + year + "/*.csv");

            //Remove header
            header = yearData.first();
            yearData = yearData.filter(line -> !line.equals(header));

            //Merge data
            allData = allData.union(yearData);
        }
        System.out.println(PRINT_GREEN + "All data successfully loaded!" + PRINT_COLOR_END);
        System.out.println();
        return allData;
    }

    public JavaRDD<DataRow> loadDataset() {
        JavaRDD<DataRow> allData = sparkContext.emptyRDD();
        for (int year : years) {
            System.out.println();
            System.out.println("Loading data from year " + year + "...");

            JavaRDD<DataRow> yearData = sparkContext.textFile("datasets/" + year + "/*.csv")
                    .filter(line -> !line.equals(header))
                    .map(line -> {
                        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

                        DataRow row = new DataRow();
                        String[] rawRow = line.split(regexp);
                        row.setStation(rawRow[0].replaceAll("\"", "").trim());
                        row.setDate(rawRow[1].replaceAll("\"", "").trim());
                        row.setLatitude(Double.parseDouble(rawRow[2].replaceAll("\"", "").trim()));
                        row.setLongitude(Double.parseDouble(rawRow[3].replaceAll("\"", "").trim()));
                        row.setElevation(Double.parseDouble(rawRow[4].replaceAll("\"", "").trim()));
                        row.setName(rawRow[5].replaceAll("\"", "").trim());
                        row.setTemp(Double.parseDouble(rawRow[6].replaceAll("\"", "").trim()));
                        row.setTempAttributes(rawRow[7].replaceAll("\"", "").trim());
                        row.setDewp(Double.parseDouble(rawRow[8].replaceAll("\"", "").trim()));
                        row.setDewpAttributes(rawRow[9].replaceAll("\"", "").trim());
                        row.setSlp(Double.parseDouble(rawRow[10].replaceAll("\"", "").trim()));
                        row.setSplAttributes(rawRow[11].replaceAll("\"", "").trim());
                        row.setStp(Double.parseDouble(rawRow[12].replaceAll("\"", "").trim()));
                        row.setStpAttributes(rawRow[13].replaceAll("\"", "").trim());
                        row.setVisib(Double.parseDouble(rawRow[14].replaceAll("\"", "").trim()));
                        row.setVisibAttributes(rawRow[15].replaceAll("\"", "").trim());
                        row.setWdsp(Double.parseDouble(rawRow[16].replaceAll("\"", "").trim()));
                        row.setWdspAttributes(rawRow[17].replaceAll("\"", "").trim());
                        row.setMxspd(Double.parseDouble(rawRow[18].replaceAll("\"", "").trim()));
                        row.setGust(Double.parseDouble(rawRow[19].replaceAll("\"", "").trim()));
                        row.setMax(Double.parseDouble(rawRow[20].replaceAll("\"", "").trim()));
                        row.setMaxAttributes(rawRow[21].replaceAll("\"", "").trim());
                        row.setMin(Double.parseDouble(rawRow[22].replaceAll("\"", "").trim()));
                        row.setMinAttributes(rawRow[23].replaceAll("\"", "").trim());
                        row.setPrcp(Double.parseDouble(rawRow[24].replaceAll("\"", "").trim()));
                        row.setPrcpAttributes(rawRow[25].replaceAll("\"", "").trim());
                        row.setSndp(Double.parseDouble(rawRow[26].replaceAll("\"", "").trim()));
                        row.setFrshtt(rawRow[27].replaceAll("\"", "").trim());

                        return row;
                    });

            //Merge data
            allData = allData.union(yearData);
        }
        System.out.println(PRINT_GREEN + "All data successfully loaded!" + PRINT_COLOR_END);
        System.out.println();
        return allData;
    }

    public Dataset<Row> createDataset(JavaRDD<DataRow> data) {
        Dataset<Row> dataset = sqlContext.createDataFrame(data, DataRow.class);
        return dataset;
    }
}
