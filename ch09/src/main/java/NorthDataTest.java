import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

/**
 * Created by wanghl on 17-3-25.
 */
public class NorthDataTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("spark-sql2").getOrCreate();

        Dataset<Row> orders = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Orders.csv").as("Order");
        System.out.println("Orders has "+orders.count()+" rows");

        Dataset<Row> orderDetails = session.read().option("header", true).csv("/home/wanghl/fdps-v3/data/NW/NW-Order-Details.csv").as("OrderDetails");
        System.out.println("Order Details has "+orderDetails.count()+" rows");

        Dataset<Row> orderByCustomer= orders.groupBy("CustomerID").count();
        orderByCustomer.sort(orderByCustomer.col("count").desc()).show(5);

        Dataset<Row> orderByCountry= orders.groupBy("ShipCountry").count();
        orderByCountry.sort(orderByCountry.col("count").desc()).show(5);

        Dataset<Row> orderDetails1 = orderDetails.select(orderDetails.col("OrderID"), orderDetails.col("UnitPrice").multiply(orderDetails.col("Qty")).minus(orderDetails.col("UnitPrice").multiply(orderDetails.col("Qty")).multiply(orderDetails.col("Discount"))).as("OrderPrice"));
        orderDetails1.show(5);

        Dataset<Row> orderTot = orderDetails1.groupBy("OrderID").sum("OrderPrice").alias("OrderTotal");
        orderTot.sort("OrderID").show(5);

        Dataset<Row> orders1 = orders.join(orderTot, orders.col("OrderID").equalTo(orderTot.col("OrderID")), "inner").select(orders.col("OrderID"), orders.col("CustomerID"), orders.col("OrderDate"), orders.col("ShipCountry").alias("ShipCountry"), orderTot.col("sum(OrderPrice)").alias("Total"));
        orders1.sort("CustomerID").show();
        orders1.filter(orders1.col("Total").isNull()).show();

        /*Dataset<Row> ordersNew = orders1.map((MapFunction<Row, Row>) row -> {
            String orderDate = row.getString(2);
            String[] parts = orderDate.split("/");
            String newOrderDate = parts[2] + "-" + parts[0] + "-" + parts[1];
            return RowFactory.create(row.get(0), row.get(1), newOrderDate, row.get(3), row.get(4), row.get(5));
        }, new Encoder<Row>() {
            @Override
            public StructType schema() {
               return ((Row)orders1.head()).schema();
            }

            @Override
            public ClassTag<Row> clsTag() {
                return null;
            }
        });

        ordersNew.show();*/

        Dataset<Row> orders2 = orders1.withColumn("Date", functions.to_date(orders1.col("OrderDate")));
        orders2.show();
        orders2.printSchema();

        Dataset<Row> orders3 =  orders2.withColumn("Month", functions.month(orders2.col("OrderDate"))).withColumn("Year", functions.year(orders2.col("OrderDate")));
        orders3.show(2);

        Dataset<Row> ordersByYM = orders3.groupBy("Year","Month").sum("Total").as("Total");
        ordersByYM.sort(ordersByYM.col("Year"),ordersByYM.col("Month")).show();

        Dataset<Row> ordersByCY = orders3.groupBy("CustomerID","Year").sum("Total").as("Total");
        ordersByCY.sort(ordersByCY.col("CustomerID"),ordersByCY.col("Year")).show();

        ordersByCY = orders3.groupBy("CustomerID","Year").avg("Total").as("Total");
        ordersByCY.sort(ordersByCY.col("CustomerID"),ordersByCY.col("Year")).show();

        Dataset<Row> ordersCA = orders3.groupBy("CustomerID").avg("Total").as("Total");
        ordersCA.sort(ordersCA.col("avg(Total)").desc()).show();

        ordersCA.write().parquet("");
    }
}
