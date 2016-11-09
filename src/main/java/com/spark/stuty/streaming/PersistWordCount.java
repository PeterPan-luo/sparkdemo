package com.spark.stuty.streaming;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.base.Optional;

/**
 * 基于持久化机制的实时wordcount程序
 * @author user
 *
 */
public class PersistWordCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
	
		streamingContext.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");
		// 然后先实现基础的wordcount逻辑
		JavaReceiverInputDStream<String> inputDStream = streamingContext.socketTextStream("localhost", 9999);
		JavaDStream<String> words = inputDStream.flatMap(new FlatMapFunction<String, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				
				return Arrays.asList(t.split(" "));
			}
		});
		JavaPairDStream<String, Integer> pairDStream = words.mapToPair(new PairFunction<String, String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		JavaPairDStream<String, Integer> wordCounts = pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

		
			private static final long serialVersionUID = 1L;

			
			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state)
					throws Exception {
				Integer newVal = 0;
				if (state.isPresent()) {
					newVal = state.get();
				}
				for(Integer val : values){
					newVal += val;
				}
				return Optional.of(newVal);
			}
		});
		// 每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化，以便于后续的J2EE应用程序
		// 进行显示
		wordCounts.foreachRDD(new Function<JavaPairRDD<String,Integer>, Void>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<String, Integer> wordCountDRR) throws Exception {
				wordCountDRR.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
					
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
						// 给每个partition，获取一个连接
						Connection conn = ConnectionPool.getConnection();
						
						// 遍历partition中的数据，使用一个连接，插入数据库
						Tuple2<String, Integer> wordCount = null;
						while(wordCounts.hasNext()){
							wordCount = wordCounts.next();
							String sql = "insert into wordcount(word,count) "
									+ "values('" + wordCount._1 + "'," + wordCount._2 + ")";  
							Statement statement = conn.createStatement();
							statement.execute(sql);
						}
						// 用完以后，将连接还回去
						ConnectionPool.returnConnection(conn);
					}
				});
				return null;
			}
		});
		wordCounts.print();
		streamingContext.start();
		streamingContext.awaitTermination();
		streamingContext.stop();

	}

}
