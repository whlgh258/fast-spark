import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by wanghl on 17-3-25.
 */
public class GraphCreateTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");

        List<Tuple2<Long, Person>> vertexList = Arrays.asList(new Tuple2<Long, Person>(1L, new Person("Alice", 18)),
                new Tuple2<Long, Person>(2L, new Person("Bernie", 17)),
                new Tuple2<Long, Person>(3L, new Person("Cruz", 15)),
                new Tuple2<Long, Person>(4L, new Person("Donald", 12)),
                new Tuple2<Long, Person>(5L, new Person("Ed", 15)),
                new Tuple2<Long, Person>(6L, new Person("Fran", 10)),
                new Tuple2<Long, Person>(7L, new Person("Genghis",854))
                );

        List<Edge> edgeList = Arrays.asList(new Edge(1L, 2L, 5),
                new Edge(1L, 3L, 1),
                new Edge(3L, 2L, 5),
                new Edge(2L, 4L, 12),
                new Edge(4L, 5L, 4),
                new Edge(5L, 6L, 2),
                new Edge(6L, 7L, 2),
                new Edge(7L, 4L, 5),
                new Edge(6L, 4L, 4)
                );

        JavaRDD<Tuple2<Long, Person>> vertexRDD = sc.parallelize(vertexList);
        JavaRDD<Edge> edgeRDD = sc.parallelize(edgeList);

//        Graph graph = new Graph() {
    }
}
