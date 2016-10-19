package com.spark.stuty.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * action操作实战
 * @author user
 *
 */
public class ActionOperation {

	public static void main(String[] args) {
		//reduce();
		//collect();
		//count();
		//take();
		countByKey();
	}
	
	private static void reduce() {
		SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRdd = sc.parallelize(numberList);
		int sum = numberRdd.reduce(new Function2<Integer, Integer, Integer>() {
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1 + v2;
			}
		});
		System.out.println("sum:" + sum);
		sc.close();
	}
	
	private static void collect() {
		SparkConf conf = new SparkConf().setAppName("collect").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRdd = sc.parallelize(numberList);
		JavaRDD<Integer> numbers = numberRdd.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				return v1 *2;
			}
		});
		// 不用foreach action操作，在远程集群上遍历rdd中的元素
		// 而使用collect操作，将分布在远程集群上的doubleNumbers RDD的数据拉取到本地
		// 这种方式，一般不建议使用，因为如果rdd中的数据量比较大的话，比如超过1万条
		// 那么性能会比较差，因为要从远程走大量的网络传输，将数据获取到本地
		// 此外，除了性能差，还可能在rdd中数据量特别大的情况下，发生oom异常，内存溢出
		// 因此，通常，还是推荐使用foreach action操作，来对最终的rdd元素进行处理
		List<Integer> collectData = numbers.collect();
		for(Integer va : collectData){
			System.out.println("data:" + va);
		}
		sc.close();
	}
	
	private static void count() {
		SparkConf conf = new SparkConf().setAppName("count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRdd = sc.parallelize(numberList);
		// 对rdd使用count操作，统计它有多少个元素
		long count =  numberRdd.count();
		System.out.println("count:" + count);
		sc.close();
	}
	
	private static void take() {
		SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRdd = sc.parallelize(numberList);
		List<Integer> top3Number = numberRdd.take(3);
		for(Integer va : top3Number){
			System.out.println("data:" + va);
		}
		sc.close();
	}
	private static void saveAsTextFile() {
		// 创建SparkConf和JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("saveAsTextFile");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 有一个集合，里面有1到10,10个数字，现在要对10个数字进行累加
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		// 使用map操作将集合中所有数字乘以2
		JavaRDD<Integer> doubleNumbers = numbers.map(
				
				new Function<Integer, Integer>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Integer call(Integer v1) throws Exception {
						return v1 * 2;
					}
					
				});
		
		// 直接将rdd中的数据，保存在HFDS文件中
		// 但是要注意，我们这里只能指定文件夹，也就是目录
		// 那么实际上，会保存为目录中的/double_number.txt/part-00000文件
		doubleNumbers.saveAsTextFile("hdfs://spark1:9000/double_number.txt");   
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
	private static void countByKey() {
		
		SparkConf conf = new SparkConf().setAppName("countByKey").setMaster("local");  
	    JavaSparkContext sc = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> dataList = Arrays.asList(new Tuple2<String, Integer>("class1", 70),
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 70),
				new Tuple2<String, Integer>("class3", 70),
				new Tuple2<String, Integer>("class3", 80),
				new Tuple2<String, Integer>("class4", 70),
				new Tuple2<String, Integer>("class5", 70),
				new Tuple2<String, Integer>("class6", 70),
				new Tuple2<String, Integer>("class6", 90));
		JavaPairRDD<String, Integer> dataPairRDD = sc.parallelizePairs(dataList);
		Map<String, Object> countByKey = dataPairRDD.countByKey();
		for(Entry<String, Object> entry : countByKey.entrySet()){
			System.out.println(entry.getKey() + " counts:" + entry.getValue());
		}
		sc.close();
	}
}
