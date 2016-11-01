package com.spark.stuty.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 * @author user
 *
 */
public class DataFrameCreate {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameCreate");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame df = sqlContext.read().json("D://sparkData/students.json");
		df.show();
	}

}
