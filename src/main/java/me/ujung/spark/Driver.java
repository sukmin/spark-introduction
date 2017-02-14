package me.ujung.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author sukmin.kwon
 * @since 2017-01-17
 */
public class Driver {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("args need input file, output file");
			return;
		}

		String inputPath = args[0];
		System.out.println("input file : " + inputPath);
		String outputPath = args[1];
		System.out.println("output file : " + outputPath);

		SparkSession spark = SparkSession.builder()
			.appName("WordCount")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		// 파일에서 RDD를 만듬
		JavaRDD<String> sampleTextRdd = sc.textFile(inputPath);

		// 한줄로 존재하던 텍스트를 단어로 분리
		JavaRDD<String> wordRdd = sampleTextRdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator(); //구버전 스파크(러닝스파크)와 다름.
			}
		});

		// . , ; ' " 제거
		JavaRDD<String> replaceWordRdd = wordRdd.map(new Function<String, String>() {
			@Override
			public String call(String word) throws Exception {
				return word.replace(",", "")
					.replace(".", "")
					.replace("'", "")
					.replace("\"", "")
					.replace(";", "");
			}
		});

		// 사이즈없는 문자열 제거
		JavaRDD<String> withoutWhiteSpaceWordRdd = replaceWordRdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String word) throws Exception {
				return word.length() != 0;
			}
		});

		// 단어마다 돌며 단어가 등장할때마다 '하나'라고 카운트 함
		JavaPairRDD<String, Long> countRdd = withoutWhiteSpaceWordRdd.mapToPair(new PairFunction<String, String, Long>() {
			@Override
			public Tuple2<String, Long> call(String word) throws Exception {
				return new Tuple2<>(word, 1L);
			}
		});

		// 단어마다 '하나'라고 표기한 것들을 키를 기준으로 한 리듀스를 통하여 합침
		JavaPairRDD<String, Long> reduceRdd = countRdd.reduceByKey(new Function2<Long, Long, Long>() {
			@Override
			public Long call(Long long1, Long long2) throws Exception {
				return long1 + long2;
			}
		});

		// 워드 카운트 정렬
		JavaPairRDD<Long, String> countAndWordRdd = reduceRdd.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
			@Override
			public Tuple2<Long, String> call(Tuple2<String, Long> wordAndCount) throws Exception {
				return new Tuple2<>(wordAndCount._2(), wordAndCount._1());
			}
		}).sortByKey(false);

		// 정렬된 워드 카운트 출력
		countAndWordRdd.saveAsTextFile(outputPath);

		spark.stop();
	}

}
