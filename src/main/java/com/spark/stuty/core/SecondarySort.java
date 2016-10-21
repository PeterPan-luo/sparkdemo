package com.spark.stuty.core;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 二次排序
 * 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 * 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 * @author Administrator
 *
 */
public class SecondarySort {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> linesRdd = sc.textFile("D://sort.txt");
		JavaPairRDD<SecondarySortKey, String> pairRDD = linesRdd.mapToPair(new PairFunction<String, SecondarySortKey, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<SecondarySortKey, String> call(String t) throws Exception {
				String[] words = t.split(" ");
				if (words.length == 2) {
					SecondarySortKey secondarySort = new SecondarySortKey(Integer.valueOf(words[0]), Integer.valueOf(words[1]));
					return new Tuple2<SecondarySortKey, String>(secondarySort, t);
				}
				return null;
			}
		});
		JavaPairRDD<SecondarySortKey, String> sortPairRDD = pairRDD.sortByKey();
		JavaRDD<String> sortLineRdd = sortPairRDD.map(new Function<Tuple2<SecondarySortKey,String>, String>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
				if (v1 != null) {
					return v1._2;					
				}
				return null;
			}
		});
		sortLineRdd.foreach(new VoidFunction<String>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				if (t != null) {
					System.out.println(t);					
				}
				
			}
		});
		sc.close();
	}
}
