package com.spark.stuty.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * 分组取top3
 * @author user
 *
 */
public class GroupTop3 {

	public static <R> void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GroupTop3").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D://score.txt");
		JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] words = t.split(" ");
				return new Tuple2<String, Integer>(words[0], Integer.valueOf(words[1]));
			}
		});
		JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
		
		JavaPairRDD<String, Iterable<Integer>> groupTop3 = groupRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				
				return new Tuple2<String, Iterable<Integer>>(t._1, getTop3(t._2));
			}
		});
		
		groupTop3.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println("class: " + t._1);  
				Iterator<Integer> scoreIterator = t._2.iterator();
				while(scoreIterator.hasNext()) {
					Integer score = scoreIterator.next();
					System.out.println(score);  
				}
				System.out.println("=======================================");   
			}
			
		});
		sc.close();
	}
	/**
	 * 获取数组中前3项最大的值
	 * @param data
	 * @return
	 */
	private static Iterable<Integer> getTop3(Iterable<Integer> data) {
		List<Integer> dataList = Lists.newArrayList(data);
		Collections.sort(dataList);
		return Lists.reverse(dataList).subList(0, 3);
	}
}
