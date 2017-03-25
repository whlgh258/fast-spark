import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by wanghl on 17-3-25.
 */
public class StatTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-9").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        String filePath = "/home/wanghl/fdps-v3/data/";
        Dataset<Row> cars = session.read().option("header","true").option("inferSchema","true").csv(filePath + "car-data/car-milage.csv");
        System.out.println("Cars has "+cars.count()+" rows");

        cars.show(5);
        cars.printSchema();

        cars.describe("mpg","hp","weight","automatic").show();

        double varcor = cars.stat().corr("hp","weight");
        System.out.println(varcor);
        double varcov = cars.stat().cov("hp","weight");
        System.out.println(varcov);

        double cor = cars.stat().corr("RARatio","width");
        System.out.println(cor);
        double cov = cars.stat().cov("RARatio","width");
        System.out.println(cov);
    }
}
