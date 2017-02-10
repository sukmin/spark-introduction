package me.ujung.spark;

import java.time.LocalDateTime;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;

/**
 * @author sukmin.kwon
 * @since 2017-02-08
 */
public class ObjectFileDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("JsonFileDriver")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		JavaRDD<String> textRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/objectfile/rank.json");
		JavaRDD<Rank> rankRdd = textRdd.map(new Function<String, Rank>() {
			@Override
			public Rank call(String textLine) throws Exception {
				Gson gson = new Gson();
				Rank rank = gson.fromJson(textLine, Rank.class);
				return rank;
			}
		});
		RddUtils.collectAndPrint("rankRdd", rankRdd);

		String outputPath = "/Users/naver/git/spark-introduction/sample/objectfile/output" + LocalDateTime.now();
		rankRdd.saveAsObjectFile(outputPath); // 웬만하면 이렇게 하지 맙시다.
		System.out.println(outputPath);

		// 오브젝트 변경
		//String readObjectPath = "/Users/naver/git/spark-introduction/sample/objectfile/output2017-02-08T07:37:48.428";
		//JavaRDD<Rank> objectRdd = sc.objectFile(readObjectPath);
		//RddUtils.collectAndPrint("objectRdd", objectRdd);

		spark.stop();
	}
}
