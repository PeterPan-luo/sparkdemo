package com.spark.stuty.core;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量
 * @author user
 *
 */
public class BroadcastVariable {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8);
		JavaRDD<Integer> lines = sc.parallelize(data);
		// 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
		// 获取的返回结果是Broadcast<T>类型
		int factor = 3;
		final Broadcast<Integer> factorCast = sc.broadcast(factor);
		
		JavaRDD<Integer> multipleNumbers = lines.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value) throws Exception {
				// 使用共享变量时，调用其value()方法，即可获取其内部封装的值
				return value * factorCast.value();
			}
		});
		
		multipleNumbers.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);  
			}
			
		});
		
		sc.close();
		
	}
		
}
