import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wanghl on 17-3-24.
 */
public class LoadCsvWithCountersJavaExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Chapter-06").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final Accumulator<Integer> errors = sc.accumulator(0);

        JavaRDD<String> inFile = sc.textFile("/home/wanghl/fdps-v3/data/Line_of_numbers.csv");
        JavaRDD<Integer[]> splitLines = inFile.flatMap(x -> {
            List<Integer[]> list = new ArrayList<>();
            try {
                CSVReader reader = new CSVReader(new StringReader(x));
                String[] parsedLine = reader.readNext();
                Integer[] ints = new Integer[parsedLine.length];
                for(int i = 0; i < parsedLine.length; i++){
                    int k = Integer.parseInt(parsedLine[i]);
                    ints[i ] = k;
                }

                list.add(ints);
            }
            catch (Exception e){
                errors.add(1);
            }

            return list.iterator();
        });

        List<Integer[]> res = splitLines.collect();
        JavaDoubleRDD javaDoubleRDD = splitLines.flatMapToDouble(x -> {
            List<Double> list = new ArrayList<Double>();
            for(Integer y : x){
                list.add(y.doubleValue());
            }

            return list.iterator();
        });

        System.out.println(javaDoubleRDD.sum());
        System.out.println("Error count "+errors.value());
        System.out.println(javaDoubleRDD.stats());

        JavaDoubleRDD jdd = splitLines.mapToDouble(x -> {
            double ret = 0;
            for(int i = 0; i < x.length; i++){
                ret += x[i];
            }

            return ret;
        });

        jdd.foreach(x -> System.out.println(x));
        System.out.println(jdd.sum());
        System.out.println(jdd.stats());
    }
}
