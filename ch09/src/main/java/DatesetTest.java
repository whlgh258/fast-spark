import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by wanghl on 17-3-24.
 */
public class DatesetTest {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-9").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        long now = System.nanoTime();
        System.out.println(now);

        String filePath = "/home/wanghl/fdps-v3/data/";
        Dataset<Row> cars = session.read().option("header","true").option("inferSchema","true").csv(filePath + "spark-csv/cars.csv");
        System.out.println("Cars has "+cars.count()+" rows");

        cars.show(5);
        cars.printSchema();

        cars.write().mode("overwrite").option("header","true").csv(filePath + "my-cars-out-csv.csv");
        cars.write().mode("overwrite").partitionBy("year").parquet(filePath + "my-cars-out-pqt");
    }
}
