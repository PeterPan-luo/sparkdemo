package com.spark.stuty.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * row_number()开窗函数实战
 * @author user
 *
 */
public class RowNumberWindowFunction {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RowNumberWindowFunction");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(sparkContext.sc());
		hiveContext.sql("DROP TABLE IF EXISTS sales");
		hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
				+ "product STRING,"
				+ "category STRING,"
				+ "revenue BIGINT)");  
		hiveContext.sql("LOAD DATA "
				+ "LOCAL INPATH '/usr/local/spark-study/resources/sales.txt' "
				+ "INTO TABLE sales");
		
		// 开始编写我们的统计逻辑，使用row_number()开窗函数
		// 先说明一下，row_number()开窗函数的作用
		// 其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号
		// 比如说，有一个分组date=20151001，里面有3条数据，1122，1121，1124,
		// 那么对这个分组的每一行使用row_number()开窗函数以后，三行，依次会获得一个组内的行号
		// 行号从1开始递增，比如1122 1，1121 2，1124 3
		
		DataFrame top3SalesDF = hiveContext.sql("SELECT PRODUCT, CATAGORY,REVENUE FROM (" +
				"select *,ROW_NUMBER() OVER(PARTITION BY CATAGORY ORDER BY REVENUE DESC) RANK FROM SALES) TEMP" +
				" WHERE RANK <= 3");
		// 将每组排名前3的数据，保存到一个表中
		hiveContext.sql("DROP TABLE IF EXISTS top3_sales");
		top3SalesDF.saveAsTable("top3_sales");
		sparkContext.close();
	}
}
