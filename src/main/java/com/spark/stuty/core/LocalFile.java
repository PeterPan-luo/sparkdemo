package com.spark.stuty.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用本地文件创建RDD
 * 案例：统计文本文件字数
 * @author user
 *
 */
public class LocalFile {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("D://spark.txt");
		
		JavaRDD<Integer> lengthRdd = lines.map(new Function<String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String line) throws Exception {
				return line.length();
			}
		});
		
		int lengthTotal = lengthRdd.reduce(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		System.out.println("文件总字数是： " + lengthTotal);
		sc.close();
	}
}
