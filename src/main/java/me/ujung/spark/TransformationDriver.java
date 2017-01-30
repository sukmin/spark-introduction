package me.ujung.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-01-21
 */
public class TransformationDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("Word Count")
			.config("spark.some.config.option", "some-value")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		List<Integer> myInteger = Arrays.asList(1, 2, 2, 3, 3);
		JavaRDD<Integer> myIntegerRdd = sc.parallelize(myInteger);

		List<Name> myNames = Arrays.asList(
			new Name("트럼프"),
			new Name("트럼프"),
			new Name("오바마"),
			new Name("부시"));
		JavaRDD<Name> myNamesRdd = sc.parallelize(myNames);

		// distinct() : 중복제거, 고비용
		JavaRDD<Integer> distinctMyIntegerRdd = myIntegerRdd.distinct();
		RddUtils.collectAndPrint("distinctMyIntegerRdd", distinctMyIntegerRdd);

		// distinct() 모델 활용 : 중복제거, 고비용
		JavaRDD<Name> distinctMyNamesRdd = myNamesRdd.distinct();
		RddUtils.collectAndPrint("distinctMyNamesRdd", distinctMyNamesRdd);

		// sample()
		JavaRDD<Name> sampleNamesRdd01 = myNamesRdd.sample(true, 1);
		RddUtils.collectAndPrint("true, 1", sampleNamesRdd01);
		JavaRDD<Name> sampleNamesRdd02 = myNamesRdd.sample(true, 0.5);
		RddUtils.collectAndPrint("true, 0.5", sampleNamesRdd02);

		// foreach()

		// countByValue()


		spark.stop();

	}


}
