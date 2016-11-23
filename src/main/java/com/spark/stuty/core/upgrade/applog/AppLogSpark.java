package com.spark.stuty.core.upgrade.applog;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Splitter;

/**
 * 移动端app访问流量日志分析案例
 * 日志格式：
 * 时间戳 设备编号 上行流量 下行流量
 * 每个设备（deviceID），总上行流量和总下行流量，计算之后，要根据上行流量和下行流量进行排序，需要进行倒序排序
 * 获取流量最大的前10个设备
 * @author user
 *
 */
public class AppLogSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AppLogSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 读取日志文件，并创建一个RDD
		// 使用SparkContext的textFile()方法，即可读取本地磁盘文件，或者是HDFS上的文件
		// 创建出来一个初始的RDD，其中包含了日志文件中的所有数据
		JavaRDD<String> accessLogRDD = sc.textFile("C://Users//Administrator//Desktop//access.log");
		//将日志RDD映射为key-value的格式
		JavaPairRDD<String, AccessLogInfo> accessLogPairRDD = mapAccessLogRDD2Pair(accessLogRDD);
		// 根据deviceID进行聚合操作
		// 获取每个deviceID的总上行流量、总下行流量、最早访问时间戳
		JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD = aggregateByDeviceID(accessLogPairRDD);
		//将按deviceID聚合RDD的key映射为二次排序key，value映射为deviceID
		JavaPairRDD<AccessLogSortKey, String> accessLogSortKeyPairRDD = mapRDDKey2SortedKey(aggrAccessLogPairRDD);
		// 执行二次排序操作，按照上行流量、下行流量以及时间戳进行倒序排序
		JavaPairRDD<AccessLogSortKey, String> sortedAccessLogPairRDD = accessLogSortKeyPairRDD.sortByKey(false);
		// 获取top10数据
		List<Tuple2<AccessLogSortKey, String>> top10DataList = 
				sortedAccessLogPairRDD.take(10);
		for(Tuple2<AccessLogSortKey, String> data : top10DataList) {
			System.out.println(data._2 + ": " + data._1);  
		}
		sc.close();
	}
	
	
	/**
	 * /将按deviceID聚合RDD的key映射为二次排序key，value映射为deviceID
	 * @param aggrAccessLogPairRDD
	 * @return
	 */
	private static JavaPairRDD<AccessLogSortKey, String> mapRDDKey2SortedKey(
			JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD) {
		
		return aggrAccessLogPairRDD.mapToPair(new PairFunction<Tuple2<String,AccessLogInfo>, AccessLogSortKey, String>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessLogInfo> t)
					throws Exception {
				String deviceID = t._1;
				AccessLogInfo accessLogInfo = t._2;
				AccessLogSortKey accessLogSortKey = new AccessLogSortKey(accessLogInfo.getUpTraffic(), accessLogInfo.getDownTraffic(), accessLogInfo.getTimestamp());
				return new Tuple2<AccessLogSortKey, String>(accessLogSortKey, deviceID);
			}
		});
	}


	/**
	 * 根据deviceID进行聚合操作
	 * 获取每个deviceID的总上行流量、总下行流量、最早访问时间戳
	 * @param accessLogPairRDD
	 * @return
	 */
	private static JavaPairRDD<String, AccessLogInfo> aggregateByDeviceID(
			JavaPairRDD<String, AccessLogInfo> accessLogPairRDD) {
		
		return accessLogPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public AccessLogInfo call(AccessLogInfo v1, AccessLogInfo v2)
					throws Exception {
				//获取最早的时间戳
				long earlierTime = v1.getTimestamp() > v2.getTimestamp() ? v2.getTimestamp() : v1.getTimestamp();
				long totalUpTraffic = v1.getUpTraffic() + v2.getUpTraffic();
				long totalDomnTraffic = v1.getDownTraffic() + v2.getDownTraffic();
				AccessLogInfo accessLogInfo = new AccessLogInfo(earlierTime, totalUpTraffic, totalDomnTraffic);
				return accessLogInfo;
			}
		});
	}


	/**
	 * 将日志RDD映射为key-value的格式
	 * @param accessLogRDD accessLogRDD 日志RDD
	 * @return
	 */
	private static JavaPairRDD<String, AccessLogInfo> mapAccessLogRDD2Pair(
			JavaRDD<String> accessLogRDD) {
		
		return accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, AccessLogInfo> call(String log) throws Exception {
				List<String> logSplitter = Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(log);
				// 获取四个字段
				long timeStamp = Long.valueOf(logSplitter.get(0));
				String deviceID = logSplitter.get(1);
				long upTraffic = Long.valueOf(logSplitter.get(2));
				long downTraffic = Long.valueOf(logSplitter.get(3));
				// 将时间戳、上行流量、下行流量，封装为自定义的可序列化对象
				AccessLogInfo accessLogInfo = new AccessLogInfo(timeStamp, upTraffic, downTraffic);
				return new Tuple2<String, AccessLogInfo>(deviceID, accessLogInfo);
			}
		});
	}
}
