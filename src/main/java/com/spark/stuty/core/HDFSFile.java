package com.spark.stuty.core;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.collection.generic.BitOperations.Int;

/**
 * 使用HDFS文件创建RDD
 * 案例：统计文本文件字数
 * @author user
 *
 */
public class HDFSFile {

	public static void main(String[] args) {
		// 创建SparkConf
		// 修改：去除setMaster()设置，修改setAppName()
		SparkConf conf = new SparkConf()
						.setAppName("HDFSFile")
						.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 使用SparkContext以及其子类的textFile()方法，针对HDFS文件创建RDD
		// 只要把textFile()内的路径修改为hdfs文件路径即可
		JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");
		
		JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String line) throws Exception {
				
				return line.length();
			}
		});
		
		int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		});
		
		System.out.println("文件总字数是：" + count);  
		sc.close();
	}
}
