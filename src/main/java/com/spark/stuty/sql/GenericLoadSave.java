package com.spark.stuty.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 * @author user
 *
 */
public class GenericLoadSave {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("GenericLoadSave");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame userDF = sqlContext.read().load("hdfs://spark1:9000/users.parquet");
		userDF.select("name", "favorite_color").write().save("hdfs://spark1:9000/namesAndFavColors.parquet");
	}

}
