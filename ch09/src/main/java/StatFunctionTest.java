import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameStatFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by wanghl on 17-3-25.
 */
public class StatFunctionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-9").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        String filePath = "/home/wanghl/fdps-v3/data/";
        Dataset<Row> cars = session.read().option("header","true").option("inferSchema","true").csv(filePath + "car-data/car-milage.csv");
        System.out.println("Cars has "+cars.count()+" rows");

        DataFrameStatFunctions stat = cars.stat();
        double cor = stat.corr("hp", "weight");
        System.out.println("hp to weight : Correlation = " + cor);

        double cov = stat.cov("hp", "weight");

        System.out.println("hp to weight : Covariance = " + cov);

        stat.crosstab("automatic", "NoOfSpeed").show();

        Dataset<Row> passengers = session.read().option("header","true").option("inferSchema","true").csv(filePath + "titanic3_02.csv");
        System.out.println("Passengers has " + passengers.count() + " rows");

        Dataset<Row> passengers1 = passengers.select(passengers.col("Pclass"), passengers.col("Survived"), passengers.col("Gender"), passengers.col("Age"),passengers.col("SibSp"), passengers.col("Parch"),passengers.col("Fare"));
        passengers1.show(5);
        passengers1.printSchema();

        passengers1.groupBy("Gender").count().show();
        passengers1.stat().crosstab("Survived","Gender").show();
        passengers1.stat().crosstab("Survived","SibSp").show();
        passengers1.stat().crosstab("Survived","Age").show();

        Dataset<Row> ageDist = passengers1.select(passengers1.col("Survived"), (passengers1.col("age").minus(passengers1.col("age").mod(10))).cast("int").as("AgeBracket"));
        ageDist.show();
        ageDist.stat().crosstab("Survived","AgeBracket").show();

        passengers1.select(passengers1.col("age").minus(passengers1.col("age").mod(10))).show();

    }
}
