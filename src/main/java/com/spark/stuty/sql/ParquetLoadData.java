package com.spark.stuty.sql;

import java.util.List;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet数据源之使用编程方式加载数据
 * @author user
 *
 */
public class ParquetLoadData {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ParquetLoadData");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		// 读取Parquet文件中的数据，创建一个DataFrame
		DataFrame userFrame = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users.parquet");
		// 将DataFrame注册为临时表，然后使用SQL查询需要的数据
		userFrame.registerTempTable("users");
		DataFrame userNamesDF = sqlContext.sql("select name from users");
		List<String> names = userNamesDF.javaRDD().map(new Function<Row, String>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.getString(0);
			}
		}).collect();
		
		for(String name : names){
			System.out.println(name);
		}
	}

}
