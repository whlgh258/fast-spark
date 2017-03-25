import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.MultiClassSummarizer;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;

/**
 * Created by wanghl on 17-3-25.
 */
public class ClassificationTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-11").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("local").appName("Chapter-11").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();
        System.out.println("Running Spark Version " + session.version());

        String filePath = "/home/wanghl/fdps-v3/data/";


        Dataset<Row> passengers = session.read().option("header","true").option("inferSchema","true").csv(filePath + "titanic3_02.csv");
        System.out.println("Passengers has "+passengers.count()+" rows");
        passengers.show(5);
        passengers.printSchema();

        Dataset<Row> passengers1 = passengers.select(passengers.col("Pclass"),passengers.col("Survived").cast("double").as("Survived"),passengers.col("Gender"),passengers.col("Age"),passengers.col("SibSp"),passengers.col("Parch"),passengers.col("Fare"));
        passengers1.show(5);
        StringIndexer indexer = new StringIndexer();
        indexer.setInputCol("Gender");
        indexer.setOutputCol("GenderCat");
        Dataset<Row> passengers2 = indexer.fit(passengers1).transform(passengers1);
        passengers2.show(5);

        Dataset<Row> passengers3 = passengers2.na().drop();
        System.out.println("Orig = "+passengers2.count()+" Final = "+ passengers3.count() + " Dropped = "+ (passengers2.count() - passengers3.count()));

        VectorAssembler assembler = new VectorAssembler();

        assembler.setInputCols(new String[]{"Pclass","GenderCat","Age","SibSp","Parch","Fare"});
        assembler.setOutputCol("features");
        Dataset<Row> passengers4 = assembler.transform(passengers3);
        passengers4.show(5);

        Dataset<Row>[] datasets = passengers4.randomSplit(new double[]{0.9, 0.1});
        Dataset<Row> train = datasets[0];
        Dataset<Row> test = datasets[1];
        System.out.println("Train = " + train.count()+" Test = " + test.count());

        DecisionTreeClassifier algTree = new DecisionTreeClassifier();
        algTree.setLabelCol("Survived");
        algTree.setImpurity("gini");
        algTree.setMaxBins(32);
        algTree.setMaxDepth(5);

        long startTime = System.nanoTime();
        DecisionTreeClassificationModel mdlTree = algTree.fit(train);
        System.out.println("The tree has " + mdlTree.numNodes() + " nodes");
        System.out.println(mdlTree.toDebugString());
        System.out.println(mdlTree.toString());
        System.out.println(mdlTree.featureImportances());


        Dataset<Row> predictions = mdlTree.transform(test);
        System.out.println(predictions.count());
        predictions.show(100);

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        evaluator.setLabelCol("Survived");
        evaluator.setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Accuracy = " + accuracy);
        long elapsedTime = (long) ((System.nanoTime() - startTime) / 1e9);
        System.out.println("Elapsed time: " + elapsedTime + " seconds");
    }
}
