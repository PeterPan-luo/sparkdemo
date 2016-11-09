package com.spark.stuty.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet数据源之自动推断分区
 * @author user
 *
 */
public class ParquetPartitionDiscovery {

	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ParquetPartitionDiscovery");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		DataFrame dataFrame = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet");
		dataFrame.printSchema();
		dataFrame.show();
	}

}
