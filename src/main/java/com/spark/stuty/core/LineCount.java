package com.spark.stuty.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 统计每行出现的次数
 * @author user
 *
 */
public class LineCount {


	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("D://spark.txt");
		
		JavaPairRDD<String, Integer> linePairs = lines.mapToPair(new PairFunction<String, String, Integer>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				
				return new Tuple2<String, Integer>(line, 1);
			}
		});
		
		JavaPairRDD<String, Integer> lineCounts = linePairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value1, Integer value2) throws Exception {
				// TODO Auto-generated method stub
				return value1 + value2;
			}
		});
		
		lineCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> lineCount) throws Exception {
				System.out.println(lineCount._1 + " appears :" + lineCount._2 +  " times");
			}
		});
		
		sc.close();
	}
}
