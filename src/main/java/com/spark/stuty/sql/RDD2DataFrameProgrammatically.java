package com.spark.stuty.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

/**
 * 以编程方式动态指定元数据，将RDD转换为DataFrame
 * @author user
 *
 */
public class RDD2DataFrameProgrammatically {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameProgrammatically");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> linesRdd = sc.textFile("D://sparkData/students.txt");
		JavaRDD<Row> rowRdd = linesRdd.map(new Function<String, Row>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				String[] words = v1.split(",");
				return RowFactory.create(Integer.valueOf(words[0]),words[1],Integer.valueOf(words[2]));
			}
		});
		// 第二步，动态构造元数据
		// 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
		// 或者是配置文件中，加载出来的，是不固定的
		// 所以特别适合用这种编程的方式，来构造元数据
		List<StructField> fieldTypes = Lists.newArrayList();
		fieldTypes.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		fieldTypes.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fieldTypes.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(fieldTypes);
		// 第三步，使用动态构造的元数据，将RDD转换为DataFrame
		DataFrame studentDF = sqlContext.createDataFrame(rowRdd, structType);
		// 后面，就可以使用DataFrame了
		studentDF.registerTempTable("students");  
		DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");
		List<Row> rows = teenagerDF.javaRDD().collect();
		for(Row row : rows){
			System.out.println(row);
		}
	}

}
