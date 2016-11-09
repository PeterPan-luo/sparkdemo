package com.spark.stuty.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * transformation操作实战
 * @author user
 *
 */
public class TransformationOperation {
	
	public static void main(String[] args) {
		//map();
		//filter();
		//flatMap();
		//groupByKey();
		//reduceByKey();
		//sortByKey();
		join();
		//cogroup();
	}
	
	/**
	 * map算子案例：将集合中每一个元素都乘以2
	 */
	private static void map() {
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1,2,3,4,5,6);
		JavaRDD<Integer> dataRdd = sc.parallelize(data);
		JavaRDD<Integer> numbersRdd = dataRdd.map(new Function<Integer, Integer>() {

	
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value1) throws Exception {
				// TODO Auto-generated method stub
				return value1 * 2;
			}
		});
		numbersRdd.foreach(new VoidFunction<Integer>() {
			
	
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer v1) throws Exception {
				System.out.println(v1);
			}
		});
		sc.close();
	}

	
	private static void filter() {
		SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(1,2,3,4,5,6);
		JavaRDD<Integer> dataRdd = sc.parallelize(data);
		JavaRDD<Integer> numberRdd = dataRdd.filter(new Function<Integer, Boolean>() {


			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v1) throws Exception {
				
				return v1 % 2 == 0;
			}
		});
		numberRdd.foreach(new VoidFunction<Integer>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer v1) throws Exception {
				System.out.println(v1);
			}
		});
		sc.close();
	}
	
	private static void flatMap() {
		SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> data = Arrays.asList("hello you","hello me","hello me","hello");
		JavaRDD<String> dataRdd = sc.parallelize(data);
		JavaRDD<String> numbersRdd = dataRdd.flatMap(new FlatMapFunction<String, String>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		numbersRdd.foreach(new VoidFunction<String>() {
			
	
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String v1) throws Exception {
				System.out.println(v1);
			}
		});
		sc.close();
	}

	private static void groupByKey() {
		SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
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
		JavaPairRDD<String,Iterable<Integer>> grouPairRDD = dataPairRDD.groupByKey();
		grouPairRDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				System.out.println("class: " + t._1);  
				Iterator<Integer> iterator = t._2.iterator();
				while(iterator.hasNext()){
					System.out.println(" value:" + iterator.next());
				}
				System.out.println("==============================");   
			}
		});
		sc.close();
		
	}
	
	private static void reduceByKey() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
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
		JavaPairRDD<String,Integer> PairRDD = dataPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1 + v2;
			}
		});
		PairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("class:" + t._1);
				System.out.println("value:" + t._2);
				System.out.println("============================");
			}
		});
		sc.close();
	}
	
	private static void sortByKey() {
		SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
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
		JavaPairRDD<String,Integer> PairRDD = dataPairRDD.sortByKey();
		PairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("class:" + t._1);
				System.out.println("value:" + t._2);
				System.out.println("============================");
			}
		});
		sc.close();
	}
	
	private static void join() {
		SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟集合
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(1, "luo"),
				new Tuple2<Integer, String>(3, "tom"));
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 60),
				new Tuple2<Integer, Integer>(2, 70),
				new Tuple2<Integer, Integer>(3, 80),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(1, 100));
		JavaPairRDD<Integer, String> studentPairRDD = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scorePairRDD = sc.parallelizePairs(scoreList);
		// 使用join算子关联两个RDD
		// join以后，还是会根据key进行join，并返回JavaPairRDD
		// 但是JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key的类型，因为是通过key进行join的
		// 第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
		// join，就返回的RDD的每一个元素，就是通过key join上的一个pair
		// 什么意思呢？比如有(1, 1) (1, 2) (1, 3)的一个RDD
		// 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
		// 如果是cogroup的话，会是(1,((1,2,3),(4)))    
		// join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4))	
		JavaPairRDD<Integer, Tuple2<String, Integer>> joinPairRDD = studentPairRDD.join(scorePairRDD);
		joinPairRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
					throws Exception {
				System.out.println("key:" + t._1);
				System.out.println("value:" + t._2._1 + " and " + t._2._2);
				System.out.println("========================================");
			}
		});
		sc.close();
		
	}
	
	private static void cogroup() {
		SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 模拟集合
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"));
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 60),
				new Tuple2<Integer, Integer>(2, 70),
				new Tuple2<Integer, Integer>(3, 80),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(1, 100));
		JavaPairRDD<Integer, String> studentPairRDD = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scorePairRDD = sc.parallelizePairs(scoreList);
		// cogroup与join不同
		// 相当于是，一个key join上的所有value，都给放到一个Iterable里面去了 
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> groupPairRDD = studentPairRDD.cogroup(scorePairRDD);
		groupPairRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
					throws Exception {
				System.out.println("key:" + t._1);
				
				System.out.println("value:" + t._2._1 );
				Iterator<Integer> iterator = t._2._2.iterator();
				while(iterator.hasNext()){
					System.out.println(" " + iterator.next());
				}
				System.out.println("========================================");
			}
		});
		
		sc.close();
		
	}
}
