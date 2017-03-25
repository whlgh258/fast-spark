import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by wanghl on 17-3-24.
 */
public class LDSV01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-05").setMaster("spark://big-data1:7077");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = ctx.parallelize(Arrays.asList(1, 2, 4));
        System.out.println(dataRDD.count());
        System.out.println(dataRDD.take(3));


    }
}
