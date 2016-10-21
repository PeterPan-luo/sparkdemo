package com.spark.stuty.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 取最大的前3个数字
 * @author user
 *
 */
public class Top3 {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> srcDataRdd = sc.textFile("D://sort.txt");
		JavaPairRDD<Integer, String> pairRDD = srcDataRdd.mapToPair(new PairFunction<String, Integer, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				
				return new Tuple2<Integer, String>(Integer.valueOf(t.trim()), t);
			}
		});
		//降序排序
		JavaPairRDD<Integer, String> sortPairRDD = pairRDD.sortByKey(false);
		JavaRDD<String> sortKeyRdd = sortPairRDD.map(new Function<Tuple2<Integer,String>, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> v1) throws Exception {
				
				return v1._2;
			}
		});
		//取前3个
		List<String> top3Nums = sortKeyRdd.take(3);
		for(String num : top3Nums){
			System.out.println(num);
		}
		sc.close();
	}

}
