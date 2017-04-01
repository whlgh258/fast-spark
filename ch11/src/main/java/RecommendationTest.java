import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.List;

/**
 * Created by wanghl on 17-3-25.
 */
public class RecommendationTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-11").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        String filePath = "/home/wanghl/fdps-v3/data/";


        Dataset<Row> movie = session.read().option("header","true").option("inferSchema","true").text(filePath + "medium/movies.dat");
        System.out.println("movie has " + movie.count()+" rows");
        movie.show(5, false);
        movie.printSchema();

        Dataset<Row> users = session.read().option("header","true").option("inferSchema","true").text(filePath + "medium/users.dat");
        System.out.println("users has " + users.count()+" rows");
        users.show(5, false);
        users.printSchema();

        Dataset<Row> ratings = session.read().option("header","true").option("inferSchema","true").text(filePath + "medium/ratings.dat");
        System.out.println("ratings has " + ratings.count()+" rows");
        ratings.show(5, false);
        ratings.printSchema();

        Dataset<Row> ratings1 = ratings.select(functions.split(ratings.col("value"), "::")).as("value");
        ratings1.show(5);

        JavaRDD<Rating> ratings2 = ratings1.javaRDD().map(x -> {
            List<String> list = x.getList(0);
            Rating rating = new Rating(Integer.parseInt(list.get(0)), Integer.parseInt(list.get(1)), Double.parseDouble(list.get(2)));
            return rating;
        });

        ratings2.take(3).forEach(x -> System.out.println(x));
        System.out.println(ratings2.count());

        Dataset<Row> ratings3 = session.createDataFrame(ratings2, Rating.class);
        System.out.println(ratings3.count());
        ratings3.show();

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
        evaluator.setMetricName("mse");
        double mse = evaluator.evaluate(pred);
        System.out.println("Mean Squared Error = "+ mse);
        mse = pred.javaRDD().map(x -> Math.pow( (x.getDouble(1) - (double)x.getFloat(3)),2)).reduce((a, b) -> a + b) / (predictions.count() * 1.0d);
        System.out.println("Mean Squared Error (Calculated) = "+ mse);
    }
}
