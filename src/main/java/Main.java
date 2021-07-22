import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Main {
    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;

    public static void main(String[] args) {
        config();

        enterAltTermBuffer();

        Data data = new Data(sparkContext, sqlContext);
        JavaRDD<String> dataString = data.loadData();
        JavaRDD<DataRow> dataRow = data.loadDataset();
        Dataset<Row> dataset = data.createDataset(dataRow);
        DataAnalysis analysis = new DataAnalysis(dataString, dataset);
        Commands commands = new Commands(analysis);

        leaveAltTermBuffer();
    }

    private static void config() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("NCDCdataAnalysis")
                .set("spark.ui.showConsoleProgress", "true");
        sparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sparkContext);
    }

    private static void enterAltTermBuffer() {
        System.out.print("\033[?1049h\033[?25l");
    }

    private static void leaveAltTermBuffer() {
        System.out.print("\033[?1049l");
        System.exit(0);
    }
}
