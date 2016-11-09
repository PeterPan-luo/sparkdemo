package com.spark.stuty.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 基于transform的实时广告计费日志黑名单过滤
 * 这里案例，完全脱胎于真实的广告业务的大数据系统，业务是真实的，实用
 * @author user
 *
 */
public class TransformBlacklist {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlacklist");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
		// 用户对我们的网站上的广告可以进行点击
		// 点击之后，是不是要进行实时计费，点一下，算一次钱
		// 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
		// 只要是黑名单中的用户点击的广告，我们就给过滤掉
		
		// 先做一份模拟的黑名单RDD
		List<Tuple2<String, Boolean>> blackList = Arrays.asList(new Tuple2<String, Boolean>("tom", Boolean.TRUE));
		
		final JavaPairRDD<String, Boolean> blackListRdd = jsc.sc().parallelizePairs(blackList);
		
		// 这里的日志格式，就简化一下，就是date username的方式
		JavaReceiverInputDStream<String> adsClickLogDStream = jsc.socketTextStream("local", 9999);
		//将日志格式转换成（name,date name）格式
		JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
		
				return new Tuple2<String, String>(t.split(" ")[0], t);
			}
		});
		
		// 然后，就可以执行transform操作了，将每个batch的RDD，与黑名单RDD进行join、filter、map等操作
		// 实时进行黑名单过滤
		JavaDStream<String> validAdsClickeDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRdd)
					throws Exception {
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClickLogRdd.leftOuterJoin(blackListRdd);
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> validRdd = joinedRDD.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {

					
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(
							Tuple2<String, Tuple2<String, Optional<Boolean>>> v1)
							throws Exception {
						//黑名单过滤掉
						if(v1._2._2.isPresent() &&
								v1._2._2.get())
							return false;
						return true;
					}
				});
				JavaRDD<String> validAdsClickedRdd = validRdd.map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {

					
					private static final long serialVersionUID = 1L;

					@Override
					public String call(
							Tuple2<String, Tuple2<String, Optional<Boolean>>> v1)
							throws Exception {
						return v1._2._1;
					}
				});
				
				return validAdsClickedRdd;
			}
		});
		
		// 打印有效的广告点击日志
		// 其实在真实企业场景中，这里后面就可以走写入kafka、ActiveMQ等这种中间件消息队列
		// 然后再开发一个专门的后台服务，作为广告计费服务，执行实时的广告计费，这里就是只拿到了有效的广告点击
		validAdsClickeDStream.print();
		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
	}
}
