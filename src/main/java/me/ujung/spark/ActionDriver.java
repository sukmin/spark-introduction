package me.ujung.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-01-22
 */
public class ActionDriver {

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

		// reuce와 fold의 차이
		Name reduceName = koreanRdd.reduce(new Function2<Name, Name, Name>() {
			@Override
			public Name call(Name name, Name name2) throws Exception {
				return new Name(name.getSource() + name2.getSource());
			}
		});
		System.out.println(reduceName);

		// 왜 초기값은 멱등성이 필요할까?
		// name은 바꾸고 넘겨도 되는데 두번째인자는 왜 바꾸지 말라고 하는가?
		Name foldName = koreanRdd.fold(new Name(""), new Function2<Name, Name, Name>() {
			@Override
			public Name call(Name name, Name name2) throws Exception {
				return new Name(name.getSource() + name2.getSource());
			}
		});
		System.out.println(foldName);

		spark.stop();

	}

}
