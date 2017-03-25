import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by wanghl on 17-3-25.
 */
public class KMeansTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-11").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        String filePath = "/home/wanghl/fdps-v3/data/";


        Dataset<Row> data = session.read().option("header","true").option("inferSchema","true").csv(filePath + "cluster-points-v2.csv");
        System.out.println("Data has " + data.count()+" rows");
        data.show(5);
        data.printSchema();

        VectorAssembler assembler = new VectorAssembler();
        assembler.setInputCols(new String[]{"X", "Y"});
        assembler.setOutputCol("features");
        Dataset data1 = assembler.transform(data);
        data1.show(5);

        KMeans algKMeans = new KMeans().setK(2);
        KMeansModel mdlKMeans = algKMeans.fit(data1);
        Dataset<Row> predictions = mdlKMeans.transform(data1);
        predictions.show(30);
//        predictions.write().mode("overwrite").option("header", true).csv(filePath + "cluster-3K.csv");
        double WSSSE = mdlKMeans.computeCost(data1);
        System.out.println("Within Set Sum of Squared Errors (K=2) = " + WSSSE);
        System.out.println("Cluster Centers (K=2) : ");
        Arrays.asList(mdlKMeans.clusterCenters()).stream().forEach(x -> System.out.println(x));
        System.out.println("Cluster Sizes (K=2) : ");
        for(long l : mdlKMeans.summary().clusterSizes()){
            System.out.println(l);
        }

        long[] a =mdlKMeans.summary().clusterSizes();
        Arrays.asList(mdlKMeans.summary().clusterSizes()).stream().flatMap(x -> {
                List<Long> list = new ArrayList<Long>();
                for(long l : x){
                    list.add(l);
                }
                return list.stream();
        }).forEach(y -> System.out.println(y));

        algKMeans = new KMeans().setK(4);
        mdlKMeans = algKMeans.fit(data1);
        WSSSE = mdlKMeans.computeCost(data1);
        System.out.println("Within Set Sum of Squared Errors (K=2) = " + WSSSE);
        System.out.println("Cluster Centers (K=2) : ");
        Arrays.asList(mdlKMeans.clusterCenters()).stream().forEach(x -> System.out.println(x));
        System.out.println("Cluster Sizes (K=2) : ");
        for(long l : mdlKMeans.summary().clusterSizes()){
            System.out.println(l);
        }

        Arrays.asList(mdlKMeans.summary().clusterSizes()).stream().flatMap(x -> {
            List<Long> list = new ArrayList<Long>();
            for(long l : x){
                list.add(l);
            }
            return list.stream();
        }).forEach(y -> System.out.println(y));
    }
}
