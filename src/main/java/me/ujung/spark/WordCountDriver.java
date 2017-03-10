package me.ujung.spark;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-03-08
 */
public class WordCountDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.appName("WordCountDriver")
			.config("spark.sql.warehouse.dir","/Users/naver/git/spark-introduction/spark-warehouse")
			.master("local[*]")
			.getOrCreate();


		String samplePath = "/Users/naver/git/spark-introduction/sample1.txt";
		Dataset<Row> df = spark.read().text(samplePath);
		df.show();

		//중요 org.apache.spark.sql.functions.

		Dataset<Row> wordDF = df.select(explode(split(col("value"), " ")).as("word"));
		wordDF.show();
		Dataset<Row> result = wordDF.groupBy("word").count();
		result.show();

		//String outputPath = "/Users/naver/git/spark-introduction/result/output";
		//result.toJSON().write().text(outputPath);

		result.write().partitionBy("count").mode(SaveMode.Overwrite).saveAsTable("wordcount");

		//!connect jdbc:hive2://127.0.0.1:10000
		//create table wordcount (word STRING, count INT) STORED AS PARQUET;
		//select * from wordcount;
		spark.stop();
	}
}
