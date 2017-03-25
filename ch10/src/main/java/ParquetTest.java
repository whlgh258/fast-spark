import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by wanghl on 17-3-25.
 */
public class ParquetTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("spark-sql2").getOrCreate();

        Dataset<Row> cars = session.read().parquet("/home/wanghl/fdps-v3/data/my-cars-out-pqt");
        cars.show();

        Dataset<Row> orders = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Orders.csv").as("Order");
        System.out.println("Orders has "+orders.count()+" rows");

        Dataset<Row> orderDetails = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Order-Details.csv").as("OrderDetails");
        System.out.println("Order Details has "+orderDetails.count()+" rows");

        orders.createOrReplaceTempView("OrdersTable");
        orderDetails.createOrReplaceTempView("OrderDetailsTable");
        Dataset<Row> result = session.sql("SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales FROM OrdersTable INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID GROUP BY ShipCountry");
        result.show(3);
        String filePath = "/home/wanghl/fdps-v3/data/";
        result.write().parquet(filePath + "SalesByCountry_Parquet");
    }
}
