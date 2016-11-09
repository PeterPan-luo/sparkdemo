package com.spark.stuty.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive数据源
 * @author user
 *
 */
public class HiveDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("HiveDataSource");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		// 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
		HiveContext context = new HiveContext(sparkContext.sc());
		
		// 判断是否存在student_infos表，如果存在则删除
		context.sql("DROP TABLE IF EXISTS STUDENT_INFO");
		
		context.sql("CREATE TABLE IF NOT EXISTS STUDENT_INFO(NAME STRING, AGE INT)");
		// 将学生基本信息数据导入student_infos表
		context.sql("LOAD DATA"+
					" LOCAL INPATH 'usr/local/spark-study/resources/student_infos.txt' " +
					" INTO TABLE STUDENT_INFO");
		// 用同样的方式给student_scores导入数据
		context.sql("DROP TABLE IF EXISTS STUDENT_SCORE"); 
		context.sql("CREATE TABLE IF NOT EXISTS STUDENT_SCORE (NAME STRING, SCORE INT)");  
		context.sql("LOAD DATA "
				+ "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
				+ "INTO TABLE STUDENT_SCORE");
		
		// 第二个功能，执行sql还可以返回DataFrame，用于查询
		// 执行sql查询，关联两张表，查询成绩大于80分的学生
		DataFrame goodStudentDF = context.sql("SELECT SI.NAME, SI.AGE,SS.SCORE FROM STUDENT_INFO SI JOIN "
												+ " STUDENT_SCORE SS ON SI.NAME = SS.NAME WHERE SS.SCORE > 80");
		// 第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
		// 将DataFrame中的数据保存到hive表中
		context.sql("DROP TABLE IF EXISTS GOOD_STUDENT_INFO");
		goodStudentDF.saveAsTable("GOOD_STUDENT_INFO");
		// 第四个功能，可以用table()方法，针对hive表，直接创建DataFrame
		Row[] goodStudentRows  = context.table("GOOD_STUDENT_INFO").collect();
		for(Row row : goodStudentRows){
			System.out.println(row);
		}
		sparkContext.close();
	}
}
