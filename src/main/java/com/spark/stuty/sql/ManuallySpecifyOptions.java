package com.spark.stuty.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 * @author user
 *
 */
public class ManuallySpecifyOptions {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("GenericLoadSave");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame peopleDF = sqlContext.read().format("json").load("hdfs://spark1:9000/people.json");
		
		peopleDF.select("name", "favorite_color").write().format("parquet").save("hdfs://spark1:9000/peopleName_java");
	}

}
