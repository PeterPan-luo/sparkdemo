package com.spark.stuty.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 排序的wordcount程序
 * @author user
 *
 */
public class SortWordCount {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D://spark.txt");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
			
				return Arrays.asList(t.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> wordPairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCounts = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1 + v1;
			}
		});
		
		//倒转key和value位置
		JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t)
					throws Exception {
				
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		});
		//根据key进行排序
		JavaPairRDD<Integer, String> sortCoutnWordRdd = countWords.sortByKey(true);
		//排完序后,倒转key和value的位置
		JavaPairRDD<String, Integer> sortWordCount = sortCoutnWordRdd.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> t)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(t._2, t._1);
			}
		});
		
		sortWordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("key:" + t._1 + " count:" + t._2);
			}
		});
		sc.close();
	}

}
