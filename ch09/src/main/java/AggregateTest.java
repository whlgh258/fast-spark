import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Created by wanghl on 17-3-24.
 */
public class AggregateTest {
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

        Dataset<Row> describe = cars.describe("mpg", "displacement", "hp", "torque");
        describe.show();

        cars.groupBy("automatic").avg("mpg","torque").show();

        cars.groupBy().avg("mpg","torque").show();

        cars.agg(functions.avg(cars.col("mpg")), functions.mean(cars.col("torque")) ).show();

        cars.groupBy("automatic").avg("mpg","torque","hp","weight").show();
    }
}
