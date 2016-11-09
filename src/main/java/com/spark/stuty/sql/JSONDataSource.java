package com.spark.stuty.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * JSON数据源
 * @author user
 *
 */
public class JSONDataSource {

	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("JSONDataSource");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		DataFrame studentsDF = sqlContext.read().json("D://sparkData/student_score.json");
		studentsDF.registerTempTable("student_score");
		DataFrame goodStudent = sqlContext.sql("select name,score from student_score where score >= 80");
		List<String> goodStudentNames = goodStudent.javaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				
				return row.getString(0);
			}
		}).collect();
		// 然后针对JavaRDD<String>，创建DataFrame
		// （针对包含json串的JavaRDD，创建DataFrame）
		List<String> studentInfoJSONs = new ArrayList<String>();
		studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");  
		studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");  
		studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");
		JavaRDD<String> studentInfoRdd = sparkContext.parallelize(studentInfoJSONs);
		DataFrame studetnInfoDF = sqlContext.read().json(studentInfoRdd);
		studetnInfoDF.registerTempTable("student_info");
		String sql = "select name,age from student_info where name in (";
		for(int i = 0 ; i < goodStudentNames.size(); i++){
			sql +=  "'" + goodStudentNames.get(i) + "'";
			if(i != goodStudentNames.size() - 1)
				sql += ",";
		}
		sql += " )";
		DataFrame goodStudentInfoDF = sqlContext.sql(sql);
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentRdd = goodStudent.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				
				return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
			}
		}).join(goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
			
				return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
			}
		}));
		// 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
		// （将JavaRDD，转换为DataFrame）name, score age
		JavaRDD<Row> goodStudentRowRdd = goodStudentRdd.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1)
					throws Exception {
				
				return RowFactory.create(v1._1,v1._2._1,v1._2._2);
			}
		});
		
		List<StructField> fieldTypes = Lists.newArrayList();
		fieldTypes.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fieldTypes.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		fieldTypes.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(fieldTypes);
		DataFrame goodStudetDF = sqlContext.createDataFrame(goodStudentRowRdd, structType);
		// 将好学生的全部信息保存到一个json文件中去
		// （将DataFrame中的数据保存到外部的json文件中去）
		goodStudetDF.write().json("D://sparkData/good_student");
	}

}
