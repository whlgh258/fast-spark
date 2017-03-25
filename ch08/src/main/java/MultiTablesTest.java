import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by wanghl on 17-3-24.
 */
public class MultiTablesTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("spark-sql2").getOrCreate();
        System.out.println(session.version());
        Dataset<Row> orders = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Orders.csv").as("Order");
        System.out.println("Orders has "+orders.count()+" rows");
        orders.show(5);
        Tuple2<String, String>[] types = orders.dtypes();
        Arrays.asList(types).stream().forEach(x -> System.out.println(x));

        Dataset<Row> orderDetails = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Order-Details.csv").as("OrderDetails");
        System.out.println("Order Details has "+orderDetails.count()+" rows");
        orderDetails.show(5);
        types = orderDetails.dtypes();
        Arrays.asList(types).stream().forEach(x -> System.out.println(x));

        orders.createOrReplaceTempView("OrdersTable");
        Dataset<Row> result = session.sql("SELECT * from OrdersTable");
        result.show(10);

        orderDetails.createOrReplaceTempView("OrderDetailsTable");
        result = session.sql("SELECT * from OrderDetailsTable");
        result.show(10);

        result = session.sql("SELECT OrderDetailsTable.OrderID, ShipCountry, UnitPrice, Qty, Discount FROM OrdersTable INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID");
        result.show(10);

        result = session.sql("SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales FROM OrdersTable INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID GROUP BY ShipCountry order by ProductSales desc");
        System.out.println(result.count());
        result.show(10);

    }
}
