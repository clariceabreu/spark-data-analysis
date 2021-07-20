import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    private static JavaSparkContext sparkContext;

    public static void main(String[] args) {
        config();

        enterAltTermBuffer();

        JavaRDD<String> data = Dataset.loadData(sparkContext);
        DataAnalysis analysis = new DataAnalysis(data);
        Commands commands = new Commands(analysis);

        leaveAltTermBuffer();
    }

    private static void config() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                                             .setAppName("NCDCdataAnalysis")
                                             .set("spark.ui.showConsoleProgress", "true");
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
