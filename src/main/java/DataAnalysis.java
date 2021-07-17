import org.apache.spark.api.java.JavaRDD;

public class DataAnalysis {
    private JavaRDD<String> data;

    public DataAnalysis(JavaRDD<String> data) {
        this.data = data;
    }

    public void mean(DatasetColumn column) {
        System.out.println("Calculating mean...");

        long startTime = System.currentTimeMillis();
        JavaRDD<String> columnData = getColumnData(column);

        long startTimeReduce = System.currentTimeMillis();
        String sum = columnData.reduce((a, b) -> {
            Float num = Float.parseFloat(a) +  Float.parseFloat(b);
            return num.toString();
        });
        long durationReduce = System.currentTimeMillis() - startTime;
        System.out.println("Time to reduce:" + durationReduce/1000);

        Float total = Float.parseFloat(sum)/columnData.count();

        System.out.println("Result: " + total);
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Time to complete operation:" + duration/1000);

        //To tentando instalar esse package Statistics do spark mas ainda não consegui
        //MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        //System.out.println(summary.mean());  // a dense vector containing the mean value for each column
    }

    private JavaRDD<String> getColumnData(DatasetColumn column) {
        long startTime = System.currentTimeMillis();
        //Regex to split csv columns (prevent from splitting columns that have comma in its content into two columns)
        String regexp = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

        //Split line to get only data from a specific column, then removing quotation marks and white spaces
        JavaRDD<String> columnData = data.map(s -> {
            try {
                String newValue = s.split(regexp)[column.index].replaceAll("\"", "").trim();
                return newValue;
            } catch (Exception e) {
                return "0";
            }
        });

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Time to map data:" + duration/1000);
        return  columnData;
    }
}
