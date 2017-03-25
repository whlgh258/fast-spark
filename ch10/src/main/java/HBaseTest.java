import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.internal.config.ConfigBuilder;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by wanghl on 17-3-25.
 */
public class HBaseTest {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("spark-sql2").getOrCreate();

        Configuration config = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, "test");
        HBaseAdmin admin = new HBaseAdmin(config);
        System.out.println(admin.isTableAvailable("test"));

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(config, TableInputFormat.class, org.apache.hadoop.hbase.io.ImmutableBytesWritable.class, org.apache.hadoop.hbase.client.Result.class);
        System.out.println(hBaseRDD.count());
        hBaseRDD.foreach(x -> System.out.println(x));

        JavaPairRDD<String, String> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2("row4", "value4")));

        JobConf jobConfig = new JobConf(config, HBaseTest.class);
        jobConfig.setOutputFormat(TableOutputFormat.class);
        jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "test");
        pairs.mapToPair(x -> convert(x)).saveAsHadoopDataset(jobConfig);

        ClusterStatus status = admin.getClusterStatus();
        System.out.println("HBase Version : " + status.getHBaseVersion());
        System.out.println("Average Load : "+ status.getAverageLoad());
        System.out.println("Backup Master Size : " + status.getBackupMastersSize());
        System.out.println("Balancer On : " + status.getBalancerOn());
        System.out.println("Cluster ID : "+ status.getClusterId());
        System.out.println("Server Info : " + status.getServerInfo());

    }

    public static Tuple2<ImmutableBytesWritable, Put> convert(Tuple2<String, String> tuple){
        Put put = new Put(Bytes.toBytes(tuple._1));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("d"), Bytes.toBytes(tuple._2));

        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
}
