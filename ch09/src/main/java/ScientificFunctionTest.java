import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;

/**
 * Created by wanghl on 17-3-25.
 */
public class ScientificFunctionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-9").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();

        JavaRDD<Number> aRDD = sc.parallelize(Arrays.asList(new Number(10), new Number(100), new Number(1000)));
        Dataset<Row> ds = session.createDataFrame(aRDD, Number.class);
        ds.show();

        ds.select( ds.col("number"), functions.log(ds.col("number")).as("ln")).show();
        ds.select( ds.col("number"), functions.log10(ds.col("number")).as("log10")).show();
        ds.select( ds.col("number"), functions.sqrt(ds.col("number")).as("sqrt")).show();

        String filePath = "/home/wanghl/fdps-v3/data/";
        Dataset<Row> data = session.read().option("header","true").option("inferSchema","true").csv(filePath + "hypot.csv");
        System.out.println("Data has "+data.count()+" rows");
        data.show(5);
        data.printSchema();
        data.select(data.col("X"), data.col("Y"), functions.hypot(data.col("X"), data.col("Y")).as("hypot")).show();
    }
}
