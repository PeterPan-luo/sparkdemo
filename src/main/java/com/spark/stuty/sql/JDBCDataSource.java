package com.spark.stuty.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class JDBCDataSource {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("JDBCDataSource");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		// 总结一下
		// jdbc数据源
		// 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
		// 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
		// 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中
		
		// 分别将mysql中两张表的数据加载为DataFrame
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://spark:3306/testdb");
		options.put("dbtable", "student_score");
		DataFrame studentScoreDF = sqlContext.read().format("jdbc").options(options).load();
		
		options.put("dbtable", "student_info");
		DataFrame studentInfoDF = sqlContext.read().format("jdbc").options(options).load();
		
		// 将两个DataFrame转换为JavaPairRDD，执行join操作,(name,<score,age>)
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> studentRdd = studentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

		
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
			}
		}).join(studentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
			}
		}));
		
		// 过滤出分数大于80分的数据
		JavaPairRDD<String, Tuple2<Integer, Integer>> hightScoreRdd = studentRdd.filter(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Boolean>() {
			
		
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<Integer, Integer>> v1)
					throws Exception {
				if(v1._2._1 >= 80)
					return true;
				else 
					return false;
			}
		});
		//转换为row rdd
		JavaRDD<Row> hightScoreRowRdd = hightScoreRdd.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1)
					throws Exception {
				
				return RowFactory.create(v1._1, v1._2._1, v1._2._2);
			}
		});
		
		// 转换为DataFrame
		List<StructField> fields = Lists.newArrayList();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(fields);
		
		DataFrame hightScoreDF = sqlContext.createDataFrame(hightScoreRowRdd, structType);
		
		Row[] rows = hightScoreDF.collect();
		for(Row row : rows) {
			System.out.println(row);  
		}
		// 将DataFrame中的数据保存到mysql表中
		// 这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
		hightScoreDF.javaRDD().foreach(new  VoidFunction<Row>() {
			
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				String sql = "insert into good_student_infos values(" 
						+ "'" + String.valueOf(row.getString(0)) + "',"
						+ Integer.valueOf(String.valueOf(row.get(1))) + ","
						+ Integer.valueOf(String.valueOf(row.get(2))) + ")";   
				Class.forName("com.mysql.jdbc.Driver");
				Connection connection= null;
				Statement statement = null;
				try {
					connection = DriverManager.getConnection("jdbc:mysql://spark01:3306/testdb","","");
					statement = connection.createStatement();
					statement.executeUpdate(sql);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					if(statement != null)
						statement.close();
					if(connection != null)
						connection.close();
				}
				
			}
		});
		
		sparkContext.close();
		
	}

}
