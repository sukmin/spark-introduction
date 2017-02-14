package me.ujung.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * @author sukmin.kwon
 * @since 2017-02-12
 */
public class MissingDriver {

	public static void main(String[] args) throws InterruptedException, IOException {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("MissingDriver")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");


		JavaRDD<String> errorRdd = sc.textFile("/Users/naver/git/spark-introduction/error.json");


		// 1. flatmap과 map과의 관계
		// map을 이용
		JavaRDD<String> errorMapRdd = errorRdd.map(new Function<String, String>() {
			@Override
			public String call(String textLine) throws Exception {
				Gson gson = new Gson();
				try {
					Rank rank = gson.fromJson(textLine, Rank.class);
					return rank.getContentsArticle().getEscapeTitle();
				} catch (JsonSyntaxException e) {
					return null;
				}
			}
		}).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s != null;
			}
		});
		RddUtils.collectAndPrint("errorMapRdd", errorMapRdd);

		// flatmap을 이용 : 일종의 필터링 역할도 가능하다.
		JavaRDD<String> errorFlatMapRdd = errorMapRdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String textLine) throws Exception {
				Gson gson = new Gson();
				try {
					Rank rank = gson.fromJson(textLine, Rank.class);
					return Arrays.asList(rank.getContentsArticle().getEscapeTitle()).iterator();
				} catch (JsonSyntaxException e) {
					return new ArrayList<String>(0).iterator();
				}
			}
		});
		RddUtils.collectAndPrint("errorFlatMapRdd", errorFlatMapRdd);

		spark.stop();
	}
}
