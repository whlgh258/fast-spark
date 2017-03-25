import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Created by wanghl on 17-3-24.
 */
public class SparkSql2 {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().master("local").appName("spark-sql2").getOrCreate();
        System.out.println(session.version());
        Dataset<Row> employees = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Employees.csv").as("Employee");
        System.out.println("Employees has " + employees.count() + " rows");

        employees.show(5);
        System.out.println(employees.head());

        employees.createOrReplaceTempView("EmployeesTable");
        Dataset<Row> result = session.sql("SELECT * from EmployeesTable");
        result.show(5);
        System.out.println(result.head(3));
        Row[] rows = (Row[]) result.head(3);
        Arrays.asList(rows).stream().forEach(x -> System.out.println(x));
        employees.explain(true);

        result = session.sql("SELECT * from EmployeesTable WHERE State = 'WA'");
        result.show(5);
        result.head(3);
        rows = (Row[]) result.head(3);
        Arrays.asList(rows).stream().forEach(x -> System.out.println(x));
        result.explain(true);
    }
}
