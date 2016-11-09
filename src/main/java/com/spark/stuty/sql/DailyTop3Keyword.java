package com.spark.stuty.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 每日top3热点搜索词统计案例
 * @author user
 *
 */
public class DailyTop3Keyword {

	public static void main(String[] args) {
		SparkConf  conf = new SparkConf().setMaster("local").setAppName("DailyTop3Keyword");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		HiveContext sqlContext = new HiveContext(sparkContext.sc());
		// 伪造出一份数据，查询条件
		// 备注：实际上，在实际的企业项目开发中，很可能，这个查询条件，是通过J2EE平台插入到某个MySQL表中的
		// 然后，这里呢，实际上，通常是会用Spring框架和ORM框架（MyBatis）的，去提取MySQL表中的查询条件
		Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
		queryParamMap.put("city", Arrays.asList("beijing"));  
		queryParamMap.put("platform", Arrays.asList("android"));  
		queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));  
		
		// 根据我们实现思路中的分析，这里最合适的方式，是将该查询参数Map封装为一个Broadcast广播变量
		// 这样可以进行优化，每个Worker节点，就拷贝一份数据即可
		final Broadcast<Map<String, List<String>>> queryBroadcast = sparkContext.broadcast(queryParamMap);
		
		// 针对HDFS文件中的日志，获取输入RDD
		JavaRDD<String> rawRdd = sparkContext.textFile("hdfs://spark01:9090/spark-study/keyword.txt");
		JavaRDD<String> filteRdd = rawRdd.filter(new Function<String, Boolean>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String log) throws Exception {
				String[] logSplited = log.split(" ");
				String city = logSplited[2];
				String platform = logSplited[1];
				String version = logSplited[0];
				
				// 与查询条件进行比对，任何一个条件，只要该条件设置了，且日志中的数据没有满足条件
				// 则直接返回false，过滤该日志
				// 否则，如果所有设置的条件，都有日志中的数据，则返回true，保留日志
				
				Map<String, List<String>> cndMap = queryBroadcast.value();
				
				List<String> cities = cndMap.get("city");
				if ( !cities.contains(city)) {
					return false;
				}
				List<String> platforms = cndMap.get("platform");
				if(!platforms.contains(platform))
					return false;
				List<String> versions = cndMap.get("version");
				if(!versions.contains(version))
					return false;
				return true;
			}
		});
		
		// 过滤出来的原始日志，映射为(日期_搜索词, 用户)的格式
		JavaPairRDD<String, String> datKeywordUserRdd = filteRdd.mapToPair(new PairFunction<String, String, String>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String log) throws Exception {
				String[] logSplited = log.split(" ");
				
				return new Tuple2<String, String>(logSplited[0] + "_" + logSplited[1], logSplited[2]);
			}
		});
		// 进行分组，获取每天每个搜索词，有哪些用户搜索了（没有去重）
		JavaPairRDD<String, Iterable<String>> datKeywordUserGroupRdd = datKeywordUserRdd.groupByKey();
		//根据日期_搜索词 进行uv统计
		JavaPairRDD<String, Integer> datKeywordUvRdd = datKeywordUserGroupRdd.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, Integer>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> datKeywordUser)
					throws Exception {
				// 对用户进行去重，并统计去重后的数量
				Set<String> userSet = Sets.newHashSet(datKeywordUser._2);
				return new Tuple2<String, Integer>(datKeywordUser._1, userSet.size());
			}
		});
		
		// 将每天每个搜索词的uv数据，转换成DataFrame
		JavaRDD<Row> datKeywordUvRowRdd = datKeywordUvRdd.map(new Function<Tuple2<String,Integer>, Row>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Integer> v1) throws Exception {
				String[] datKeyWord = v1._1.split("_");
				
				return RowFactory.create(datKeyWord[0],datKeyWord[1],v1._2);
			}
		});
		List<StructField> fields = Lists.newArrayList(DataTypes.createStructField("date", DataTypes.StringType, true),
									DataTypes.createStructField("keyword", DataTypes.StringType, true),
									DataTypes.createStructField("uv", DataTypes.DoubleType, true));
		StructType structType = DataTypes.createStructType(fields);
		
		DataFrame datKeywordUvDF = sqlContext.createDataFrame(datKeywordUvRowRdd, structType);
		// 使用Spark SQL的开窗函数，统计每天搜索uv排名前3的热点搜索词
		datKeywordUvDF.registerTempTable("daily_keyword_uv");
		//
		String sqlText = "select * from " +
				"(select *,ROW_NUMBER() OVER(PARTITION BY date ORDER BY uv DESC) rank from daily_keyword_uv) temp" +
				"where rank <= 3";
		DataFrame dailyTop3KeyWord = sqlContext.sql(sqlText);
		
		// 将DataFrame转换为RDD，然后映射，计算出每天的top3搜索词的搜索uv总数
		JavaRDD<Row> dailyTop3KeywordRDD = dailyTop3KeyWord.javaRDD();
		JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordRDD.mapToPair(new PairFunction<Row, String, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				String date = String.valueOf(row.get(0));
				String keyword = String.valueOf(row.get(1));
				long uv = Long.valueOf(String.valueOf(row.get(2)));
				return new Tuple2<String, String>(date, keyword + "_" + uv);
			}
		});
		
		JavaPairRDD<String, Iterable<String>> top3DateKeywordUvGroupRDD = top3DateKeywordUvRDD.groupByKey();
		JavaPairRDD<Long, String> dailyTotalUvRdd = top3DateKeywordUvGroupRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, Long, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> t)
					throws Exception {
				String datekeyWordUv = t._1;
				long totalUV = 0;
				Iterator<String> iterator = t._2.iterator();
				while(iterator.hasNext()){
					String keyWordUv = iterator.next();
					long uv = Long.valueOf(keyWordUv.split("_")[1]);
					totalUV += uv;
					datekeyWordUv += "_" + keyWordUv;
				}
				return new Tuple2<Long, String>(totalUV, datekeyWordUv);
			}
		});
		// 按照每天的总搜索uv进行倒序排序
		JavaPairRDD<Long, String> sortedDailyTotalUvRdd = dailyTotalUvRdd.sortByKey(false);
		// 再次进行映射，将排序后的数据，映射回原始的格式，Iterable<Row>
		JavaRDD<Row> sortedRowRDD = sortedDailyTotalUvRdd.flatMap(
				
				new FlatMapFunction<Tuple2<Long,String>, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Row> call(Tuple2<Long, String> tuple)
							throws Exception {
						String dateKeywords = tuple._2;
						String[] dateKeywordsSplited = dateKeywords.split(",");  
						
						String date = dateKeywordsSplited[0];
						
						List<Row> rows = new ArrayList<Row>();
						rows.add(RowFactory.create(date, 
								dateKeywordsSplited[1].split("_")[0],
								Long.valueOf(dateKeywordsSplited[1].split("_")[1]))); 
						rows.add(RowFactory.create(date, 
								dateKeywordsSplited[2].split("_")[0],
								Long.valueOf(dateKeywordsSplited[2].split("_")[1]))); 
						rows.add(RowFactory.create(date, 
								dateKeywordsSplited[3].split("_")[0],
								Long.valueOf(dateKeywordsSplited[3].split("_")[1]))); 
						
						return rows;
					}
					
				});
		
		// 将最终的数据，转换为DataFrame，并保存到Hive表中
		DataFrame finalDF = sqlContext.createDataFrame(sortedRowRDD, structType);
		
		finalDF.saveAsTable("daily_top3_keyword_uv");
		sparkContext.close();
	}
}
