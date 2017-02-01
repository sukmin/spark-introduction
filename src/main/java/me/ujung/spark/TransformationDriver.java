package me.ujung.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple22;

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

		// subtract() : 고비용
		JavaRDD<Name> pureAmericanRdd = americanRdd.subtract(koreanRdd);
		RddUtils.collectAndPrint("pureAmericanRdd", pureAmericanRdd);

		// catesian() : 매우 고비용
		//JavaRDD<Name> cateRdd = americanRdd.cartesian()?
		//RddUtils.collectAndPrint("cateRdd",cateRdd);


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

		// 수치연산에 특화된 doubleRDD 라는 것도 있음
		JavaDoubleRDD doubleRdd = myNamesRdd.mapToDouble(new DoubleFunction<Name>() {
			@Override
			public double call(Name name) throws Exception {
				return name.getSource().length();
			}
		});
		//doubleRdd.

		// mapPartitions : 파티션별로 단 한번만 실행하고 싶은 경우
		JavaRDD<Name> mapPartitionsNamesRdd01 = myNamesRdd.mapPartitions(new FlatMapFunction<Iterator<Name>, Name>() {
			@Override
			public Iterator<Name> call(Iterator<Name> nameIterator) throws Exception {
				List<Name> afterNames = new ArrayList<>();
				while (nameIterator.hasNext()) {
					Name name = nameIterator.next();
					String originSource = name.getSource();
					name.setSource("배고픈 " + originSource);
					afterNames.add(name);
				}
				return afterNames.iterator();
			}
		});
		RddUtils.collectAndPrint("mapPartitionsNamesRdd01", mapPartitionsNamesRdd01);

		// mapPartitionsWithIndex : 파티션의 인덱스를 얻고 싶은 경우
		JavaRDD<Name> mapPartitionsWithIndexNamesRdd01 = myNamesRdd.repartition(2).mapPartitionsWithIndex(new Function2<Integer, Iterator<Name>, Iterator<Name>>() {
			@Override
			public Iterator<Name> call(Integer integer, Iterator<Name> nameIterator) throws Exception {
				List<Name> afterNames = new ArrayList<>();
				while (nameIterator.hasNext()) {
					Name name = nameIterator.next();
					String originSource = name.getSource();
					name.setSource(integer + "번파티션의 배고픈 " + originSource);
					afterNames.add(name);
				}
				return afterNames.iterator();
			}
		}, true); // 이건 없었는데?
		RddUtils.collectAndPrint("mapPartitionsWithIndexNamesRdd01", mapPartitionsWithIndexNamesRdd01);





		spark.stop();

	}

}
