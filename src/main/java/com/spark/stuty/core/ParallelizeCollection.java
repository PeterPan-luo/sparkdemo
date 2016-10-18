package com.spark.stuty.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 并行化集合创建RDD
 * 案例：累加1到10
 * @author user
 *
 */
public class ParallelizeCollection {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		JavaRDD<Integer> dataRdd = sc.parallelize(data);
		
		int sum = dataRdd.reduce(new Function2<Integer, Integer, Integer>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
								return v1 + v2;
			}
		});
		
		// 输出累加的和
		System.out.println("1到10的累加和：" + sum);  
		sc.close();
	}
}
