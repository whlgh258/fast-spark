import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by wanghl on 17-3-24.
 */
public class LoadHBase {
    public static void main(String[] args) throws IOException {
        JavaSparkContext sc = new JavaSparkContext(args[0], "sequence load", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, args[1]);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin = new HBaseAdmin(ConnectionFactory.createConnection(conf));

        if(!admin.isTableAvailable(args[1])) {
            HTableDescriptor tableDesc = new HTableDescriptor(args[1]);
            admin.createTable(tableDesc);
        }

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        hBaseRDD.saveAsTextFile("", SnappyCodec.class);
    }
}
