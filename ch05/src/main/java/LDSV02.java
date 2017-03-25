import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by wanghl on 17-3-24.
 */
public class LDSV02 {
    public static void main(String[] args) {
        // standalone模式下一直不行，worker找不到文件，在spark-shell也是如此
        SparkConf conf = new SparkConf().setAppName("Chapter 05").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        System.out.println("Running Spark Version : "+ctx.version());
        ctx.addFile("/home/wanghl/fdps-v3/data/spam.data");
        JavaRDD<String> lines = ctx.textFile(SparkFiles.get("spam.data"));
        System.out.println(lines.first());

//        JavaRDD<String> inFile = ctx.textFile("/home/wanghl/fdps-v3/data/Line_of_numbers.csv");
        ctx.addFile("/home/wanghl/fdps-v3/data/Line_of_numbers.csv");
        JavaRDD<String> inFile = ctx.textFile(SparkFiles.get("Line_of_numbers.csv"));
        JavaRDD<String[]> numbersRDD = inFile.map(x -> x.split(","));
        System.out.println(numbersRDD.take(10));

        /*JavaRDD<Double> doubleNumbersRDD = inFile.map(x -> x.split(",")).flatMap(y -> {
            List<Double> list = new ArrayList<>();
            for(String s : y){
                double d = Double.parseDouble(s);
                list.add(d);
            }

            return list.iterator();
        });

        doubleNumbersRDD.foreach(x -> System.out.println(x));*/

        /*JavaRDD<Double> doubleNumbersRDD = inFile.flatMap(x -> {
            List<Double> list = new ArrayList<Double>();
            for(String s : x.split(",")){
                double d = Double.parseDouble(s);
                list.add(d);
            }

            return list.iterator();
        });

        doubleNumbersRDD.foreach(x -> System.out.println(x));*/

//        JavaRDD<Double> doubleNumbersRDD = inFile.flatMap(x -> Arrays.asList(x.split(",")).iterator()).map(y -> Double.parseDouble(y));
//        doubleNumbersRDD.foreach(x -> System.out.println(x));

//        JavaDoubleRDD doubleNumbersRDD = inFile.flatMap(x -> Arrays.asList(x.split(",")).iterator()).mapToDouble(y -> Double.parseDouble(y));
//        System.out.println(doubleNumbersRDD.sum());

        JavaRDD<String[]> result = inFile.map(x -> {
            CSVReader reader = new CSVReader(new StringReader(x));
            return reader.readNext();
        });

        JavaDoubleRDD doubleNumbersRDD = result.flatMapToDouble(x -> {
            List<Double> list = new ArrayList<Double>();
            for(String s : x){
                list.add(Double.parseDouble(s));
            }

            return list.iterator();
        });

        System.out.println(doubleNumbersRDD.sum());
    }
}
