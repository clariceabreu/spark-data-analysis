import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    private static JavaSparkContext sparkContext;
    private static SparkSession sparkSession;

    public static void main(String[] args) {
        config();

        enterAltTermBuffer();

        JavaRDD<String> data = Data.loadData(sparkContext);
        Dataset<Row> dataset = Data.createDataset(sparkSession, data);
        DataAnalysis analysis = new DataAnalysis(data, dataset);
        Commands commands = new Commands(analysis);

        leaveAltTermBuffer();
    }

    private static void config() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("NCDCdataAnalysis")
                .set("spark.ui.showConsoleProgress", "true");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkConf);
    }

    private static void enterAltTermBuffer() {
        System.out.print("\033[?1049h\033[?25l");
    }

    private static void leaveAltTermBuffer() {
        System.out.print("\033[?1049l");
        System.exit(0);
    }
}
