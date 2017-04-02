import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-1.
 */
public class LastFMRecommendation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LsstFM").setMaster("spark://big-data1:7077").setJars(new String[]{"/home/wanghl/IdeaProjects/fast-spark/ch11/target/ch11-1.0.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession spark = SparkSession.builder().master("spark://big-data1:7077").appName("LsstFM").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + spark.version());

        JavaRDD<String> rawArtistAlias = sc.textFile("hdfs://big-data1:9000/user/ds/artist_alias.txt");
        System.out.println(rawArtistAlias.count());
        JavaPairRDD<Integer, Integer> artistsAliasPair = rawArtistAlias.mapToPair(row -> {
            String[] tokens = row.split("\\s");
            Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(0, 0);
            if(StringUtils.isNotBlank(tokens[0])){
                tuple = new Tuple2<Integer, Integer>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
            }

            return tuple;
        });


        Map<Integer, Integer> artistsAliasMap = artistsAliasPair.collectAsMap();
        Broadcast<Map<Integer, Integer>> broadcast = sc.broadcast(artistsAliasMap);

        Dataset<Row> ratings = spark.read().option("header", false).option("inferSchema", true).text("hdfs://big-data1:9000/user/ds/user_artist_data.txt");
        ratings.show(5, false);
        ratings.printSchema();
        System.out.println(ratings.count());

        Dataset<Row> ratings1 = ratings.select(functions.split(ratings.col("value"), "\\s")).as("values");
        ratings1.show(5, false);

        JavaRDD<Rating> ratings2 = ratings1.javaRDD().map(x -> {
            List<String> list = x.getList(0);
            Rating rating = new Rating(Integer.parseInt(list.get(0)), broadcast.getValue().getOrDefault(Integer.parseInt(list.get(1)), Integer.parseInt(list.get(1))), Double.parseDouble(list.get(2)));

            return rating;
        });

        ratings2.take(5).forEach(x -> System.out.println(x));

        Dataset<Row> ratings3 = spark.createDataFrame(ratings2, Rating.class);
        System.out.println("======= " + ratings3);
        System.out.println(ratings3.count());
        ratings3.show();
        ratings3.printSchema();
        Dataset<Row>[] splits = ratings3.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];
        System.out.println("Train = "+train.count()+" Test = "+test.count());

        ALS algALS = new ALS();
        algALS.setItemCol("product");
        algALS.setRank(12);
        algALS.setRegParam(0.1); // was regularization parameter, was lambda in MLlib
        algALS.setMaxIter(20);
        ALSModel mdlReco = algALS.fit(train);

        Dataset<Row> predictions = mdlReco.transform(test);
        predictions.show(5);
        predictions.printSchema();

        Dataset<Row> pred = predictions.na().drop();
        System.out.println("Orig = "+predictions.count()+" Final = "+ pred.count() + " Dropped = "+ (predictions.count() - pred.count()));
        RegressionEvaluator evaluator = new RegressionEvaluator();
        evaluator.setLabelCol("rating");
        double rmse = evaluator.evaluate(pred);
        System.out.println("Root Mean Squared Error = " + rmse);

        spark.stop();
        sc.stop();
    }
}
