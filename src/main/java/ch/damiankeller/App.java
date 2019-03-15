package ch.damiankeller;

/**
 * Hello world!
 *
 */
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.*;
public class App {

    public static void main(String[] args) {
        String sparkLocation = "~/Documents/Code/spark-2.4.0-bin-hadoop2.7";
        String appJar = "target/P9-spark-1.0-SNAPSHOT.jar";
        String logFile = "README.md"; // Should be some file on your system

        JavaSparkContext sc = new JavaSparkContext("local", "Simple App");
                //sparkLocation, new String[]{appJar});
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + "and lines with b: " + numBs);
        System.out.println("Hello Spark");
    }
}
