package me.ujung.spark;

import java.time.LocalDateTime;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import scala.tools.cmd.Spec;

/**
 * @author sukmin.kwon
 * @since 2017-02-07
 */
public class JsonFileDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("JsonFileDriver")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		JavaRDD<String> textRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/jsonfile/rank.json");

		// 이렇게 실행. gson 생성 자원을 아끼려면 저번시간에 배운 mapPartions 메소드 사용
		JavaRDD<String> titleRdd = textRdd.map(new Function<String, String>() {
			@Override
			public String call(String textLine) throws Exception {
				Gson gson = new Gson();
				Rank rank = gson.fromJson(textLine, Rank.class);
				return rank.getContentsArticle().getEscapeTitle();
			}
		});
		RddUtils.collectAndPrint("titleRdd", titleRdd);

		// 커스텀객체로 받으려면 직렬화 되어야..
		JavaRDD<Rank> rankRdd = textRdd.map(new Function<String, Rank>() {
			@Override
			public Rank call(String textLine) throws Exception {
				Gson gson = new Gson();
				Rank rank = gson.fromJson(textLine, Rank.class);
				return rank;
			}
		});
		RddUtils.collectAndPrint("rankRdd", rankRdd);

		// 이제 람다식 좀 써볼까? LocalDateTime은 Date사용이 불편해서 자바8에 추가된 클래스
		String outputPath = "/Users/naver/git/spark-introduction/sample/jsonfile/output" + LocalDateTime.now();
		rankRdd.map(rank -> {
			Gson gson = new Gson();
			return gson.toJson(rank);
		}).saveAsTextFile(outputPath);

		JavaRDD<String> errorRdd = sc.textFile("/Users/naver/git/spark-introduction/sample/jsonfile/error.json");

		// 공유메모리. 액션에서만 사용해야 정확도 보장됨. 트랜스포메이션에서 사용시 보장되진 않음.
		final LongAccumulator longAccumulator = sc.sc().longAccumulator();
		JavaRDD<String> errorTitleRdd = errorRdd.map(new Function<String, String>() {
			@Override
			public String call(String textLine) throws Exception {
				Gson gson = new Gson();
				try {
					Rank rank = gson.fromJson(textLine, Rank.class);
					return rank.getContentsArticle().getEscapeTitle();
				} catch (JsonSyntaxException e) {
					longAccumulator.add(1);
					return null;
				}
			}
		}).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s != null;
			}
		});
		RddUtils.collectAndPrint("errorTitleRdd", errorTitleRdd);
		System.out.println("json error count : " + longAccumulator.value());

		spark.stop();
	}
}
