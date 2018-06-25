package org.com.spark.SPARKTutorial;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
				
		//Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> textFile = sc.textFile("C:\\Users\\aruld\\eclipse-workspace\\SPARKTutorial\\src\\main\\java\\resources\\shakespeare.txt");
        
        JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator()).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
        
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile("/tmp/shakespeareWordCount");

	}

}
