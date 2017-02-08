package me.ujung.spark;

import java.io.IOException;
import java.time.LocalDateTime;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-02-07
 */
public class TextFileDriver {

	public static void main(String[] args) throws InterruptedException, IOException {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("Word Count")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		JavaRDD<String> filesRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/textfile");
		RddUtils.collectAndPrint("filesRdd", filesRdd);

		JavaRDD<String> textFilesRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/textfile/*-02*.txt");
		RddUtils.collectAndPrint("textFilesRdd", textFilesRdd);

		JavaPairRDD<String, String> pairRdd = sc.wholeTextFiles("/Users/naver/git/spark-introduction/sample/textfile/*-02*.txt");
		RddUtils.collectAndPrint("pairRdd", pairRdd);
		JavaRDD<String> fileNameRdd = pairRdd.keys();
		RddUtils.collectAndPrint("fileNameRdd", fileNameRdd);

		String outputPath = "/Users/naver/git/spark-introduction/sample/textfile/output" + LocalDateTime.now();
		pairRdd.saveAsTextFile(outputPath);

		spark.stop();

	}
}
