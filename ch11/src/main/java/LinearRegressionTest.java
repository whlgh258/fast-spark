import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Created by wanghl on 17-3-25.
 */
public class LinearRegressionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-11").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        String filePath = "/home/wanghl/fdps-v3/data/";
        Dataset<Row> cars = session.read().option("header","true").option("inferSchema","true").csv(filePath + "car-data/car-milage.csv");
        System.out.println("Cars has "+cars.count()+" rows");

        Dataset<Row> cars1 = cars.na().drop();
        VectorAssembler assembler = new VectorAssembler();
        assembler.setInputCols(new String[]{"displacement", "hp", "torque", "CRatio", "RARatio", "CarbBarrells", "NoOfSpeed", "length", "width", "weight", "automatic"});
        assembler.setOutputCol("features");
        Dataset<Row> cars2= assembler.transform(cars1);
        cars2.show(40);

        Dataset<Row> train = cars2.filter(cars1.col("weight").$less$eq(4000));
        Dataset<Row> test = cars2.filter(cars1.col("weight").$greater(4000));
        train.show(30);
        test.show();
        System.out.println("Train = "+train.count()+" Test = "+test.count());

        LinearRegression algLR = new LinearRegression();
        algLR.setMaxIter(100);
        algLR.setRegParam(0.3);
        algLR.setElasticNetParam(0.8);
        algLR.setLabelCol("mpg");

        LinearRegressionModel mdlLR = algLR.train(train);

        System.out.println("Coefficients: " + mdlLR.coefficients() + " Intercept: " + mdlLR.intercept());
        LinearRegressionTrainingSummary trSummary = mdlLR.summary();
        System.out.println("numIterations: " + trSummary.totalIterations());
        System.out.println("Iteration Summary History: ");
        System.out.println(trSummary.objectiveHistory().length);
        for(double d : trSummary.objectiveHistory()){
            System.out.print(d + ", ");
        }
        trSummary.residuals().show();
        System.out.println("RMSE: " + trSummary.rootMeanSquaredError());
        System.out.println("r2: " + trSummary.r2());

        Dataset<Row> predictions = mdlLR.transform(test);
        predictions.show();

        RegressionEvaluator evaluator = new RegressionEvaluator();
        evaluator.setLabelCol("mpg");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error = " + rmse);
        evaluator.setMetricName("mse");
        double mse = evaluator.evaluate(predictions);
        System.out.println("Mean Squared Error = " + mse);
    }
}
