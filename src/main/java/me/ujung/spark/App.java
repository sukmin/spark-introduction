package me.ujung.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(App.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> singers = Arrays.asList("소녀시대", "아이유", "서태지");
		JavaRDD<String> rdd = sc.parallelize(singers);

		System.out.println(rdd.count());
		System.out.println(StringUtils.join(singers));
	}
}
