package me.ujung.spark;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.SparkSession;

import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

/**
 * @author sukmin.kwon
 * @since 2017-02-09
 */
public class JdbcRddDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("JsonFileDriver")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		// 참고 : http://www.sparkexpert.com/2015/01/02/load-database-data-into-spark-using-jdbcrdd-in-java/
		JdbcRDD<String[]> jdbcRdd = new JdbcRDD<String[]>(sc.sc(), new DBConnection(),
			"SELECT * FROM DOG_TEST WHERE id > ? AND id <= ?", 0, 50, 1,
			new MyMapper(), ClassManifestFactory$.MODULE$.fromClass(String[].class));

		JavaRDD<String[]> javaRdd = jdbcRdd.toJavaRDD();
		RddUtils.collectAndPrint("javaRdd", javaRdd);

		JavaRDD<String> flatJavaRdd = javaRdd.flatMap(new FlatMapFunction<String[], String>() {
			@Override
			public Iterator<String> call(String[] strings) throws Exception {
				return Arrays.asList(strings).iterator();
			}
		});
		RddUtils.collectAndPrint("flatJavaRdd", flatJavaRdd);

		spark.stop();
	}

	public static class DBConnection extends AbstractFunction0<Connection> implements Serializable {

		@Override
		public Connection apply() {
			// 커넥션 획득하기
			String jdbcPath = "jdbc:mysql://125.209.193.177:3306/dbalphadog?Unicode=true&characterEncoding=UTF-8&autoReconnect=true&useSSL=false";
			String userName = "dog_admin";
			String password = "dogadmin";
			Connection connection = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				connection = DriverManager.getConnection(jdbcPath, userName, password);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return connection;
		}
	}

	public static class MyMapper extends AbstractFunction1<ResultSet, String[]> implements Serializable {

		@Override
		public String[] apply(ResultSet resultSet) {
			List<String> dataList = new ArrayList<>();
			try {

				while (resultSet.next()) {
					StringBuilder builder = new StringBuilder();
					builder.append(resultSet.getLong("id")).append(";");
					builder.append(resultSet.getString("message")).append(";");
					builder.append(resultSet.getDate("reg_ymdt").toString());
					dataList.add(builder.toString());
				}
			} catch (SQLException e) {
			}

			return dataList.toArray(new String[dataList.size()]);
		}
	}
}
