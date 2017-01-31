package me.ujung.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-01-22
 */
public class UnionDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("Word Count")
			.config("spark.some.config.option", "some-value")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		List<Name> american = Arrays.asList(
			new Name("유승준"),
			new Name("마이클잭슨"),
			new Name("아델"),
			new Name("저스틴비버"));
		JavaRDD<Name> americanRdd = sc.parallelize(american);

		List<Name> korean = Arrays.asList(
			new Name("유승준"),
			new Name("조용필"),
			new Name("최유정"),
			new Name("아이유"));
		JavaRDD<Name> koreanRdd = sc.parallelize(korean);

		// union()
		JavaRDD<Name> americanAndKorean = americanRdd.union(koreanRdd);
		RddUtils.collectAndPrint("americanAndKorean", americanAndKorean);

		// distinct() : 고비용
		JavaRDD<Name> distinctRdd = americanAndKorean.distinct();
		RddUtils.collectAndPrint("distinctRdd", distinctRdd);

		// intersection() : 고비용
		JavaRDD<Name> stevRdd = americanRdd.intersection(koreanRdd);
		RddUtils.collectAndPrint("stevRdd", stevRdd);

		// substract() : 고비용
		JavaRDD<Name> pureAmericanRdd = americanRdd.subtract(koreanRdd);
		RddUtils.collectAndPrint("pureAmericanRdd", pureAmericanRdd);

		// catesian() : 매우 고비용
		//JavaRDD<Name> cateRdd = americanRdd.cartesian(koreanRdd);
		//RddUtils.collectAndPrint("cateRdd",cateRdd);

		spark.stop();

	}
}
