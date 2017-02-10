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
			.appName("TextFileDriver")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		// 디렉토리 내의 모든 파일을 읽을 수 있다.
		JavaRDD<String> filesRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/textfile");
		RddUtils.collectAndPrint("filesRdd", filesRdd);

		// 패턴형식으로 지정도 가능하다.
		JavaRDD<String> textFilesRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/textfile/*-02*.txt");
		RddUtils.collectAndPrint("textFilesRdd", textFilesRdd);

		// 파일명까지 가져오고 싶은 경우 (한 머신에 모두 올릴만한 작은 파일만..)
		JavaPairRDD<String, String> pairRdd = sc.wholeTextFiles("/Users/naver/git/spark-introduction/sample/textfile/*-02*.txt");
		RddUtils.collectAndPrint("pairRdd", pairRdd);
		JavaRDD<String> fileNameRdd = pairRdd.keys();
		RddUtils.collectAndPrint("fileNameRdd", fileNameRdd);

		String outputPath = "/Users/naver/git/spark-introduction/sample/textfile/output" + LocalDateTime.now();
		pairRdd.saveAsTextFile(outputPath);

		spark.stop();

	}
}
