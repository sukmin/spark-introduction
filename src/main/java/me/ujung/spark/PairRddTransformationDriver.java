package me.ujung.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author sukmin.kwon
 * @since 2017-02-01
 */
public class PairRddTransformationDriver {

	public static void main(String[] args) throws InterruptedException {

		SparkSession spark = SparkSession.builder()
			.master("local")
			.appName("Word Count")
			.config("spark.some.config.option", "some-value")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		sc.setLogLevel("ERROR");

		List<Person> people = getPeople(100);

		JavaRDD<Person> peopleRdd = sc.parallelize(people);
		RddUtils.collectAndPrint("peopleRdd", peopleRdd);

		// 나이기준으로 페어RDD생성
		JavaPairRDD<Integer, Person> ageAndPersonRdd = peopleRdd.mapToPair(new PairFunction<Person, Integer, Person>() {
			@Override
			public Tuple2<Integer, Person> call(Person person) throws Exception {
				return new Tuple2<>(person.getAge(), person);
			}
		});
		RddUtils.collectAndPrint("ageAndPersonRdd", ageAndPersonRdd);

		// groupByKey : 비용이 아주 쎔 reduceByKey와의 차이점은?
		JavaPairRDD<Integer, Iterable<Person>> groupByKeyRdd = ageAndPersonRdd.groupByKey();
		RddUtils.collectAndPrint("groupByKeyRdd", groupByKeyRdd);

		// aggregateByKey()
		JavaPairRDD<Integer, Integer> aggregateByKeyRdd = ageAndPersonRdd.aggregateByKey(0, new Function2<Integer, Person, Integer>() {
			@Override
			public Integer call(Integer integer, Person person) throws Exception {
				return integer + 1;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});
		RddUtils.collectAndPrint("aggregateByKeyRdd", aggregateByKeyRdd);

		// combineByKey() : 저수준
		JavaPairRDD<Integer, Integer> combineByKeyRdd = ageAndPersonRdd.combineByKey(new Function<Person, Integer>() {
			@Override
			public Integer call(Person person) throws Exception {
				return 1;
			}
		}, new Function2<Integer, Person, Integer>() {
			@Override
			public Integer call(Integer integer, Person person) throws Exception {
				return integer + 1;
			}
		}, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		});
		RddUtils.collectAndPrint("combineByKeyRdd", combineByKeyRdd);

		// join() 등등은 62페이지 책을 보고..

		spark.stop();
	}

	private static List<Person> getPeople(int length) {

		String[] names = {"김유몬", "이고객", "박운영", "강개발", "구데브", "피카츄", "파이리", "꼬부기", "잠만보"};
		String[] addresses = {"서울", "평양", "부산", "대전", "춘천", "제주", "울릉도", "독도", "광주"};

		int peopleLength = length;
		Random random = new Random();
		List<Person> people = new ArrayList<>(peopleLength);
		for (int i = 1; i <= peopleLength; i++) {
			String name = names[random.nextInt(names.length - 1)];
			int age = random.nextInt(100 - 20) + 20;
			String address = addresses[random.nextInt(addresses.length - 1)];
			Person person = new Person(i, name, age, address);
			people.add(person);
		}

		return people;
	}

}
