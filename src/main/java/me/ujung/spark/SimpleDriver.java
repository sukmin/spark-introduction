package me.ujung.spark;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-03-08
 */
public class SimpleDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.appName("SimpleDriver")
			.master("local[*]")
			.getOrCreate();

		//JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		// integer
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(numbers, integerEncoder);

		//primitiveDS.show();
		//primitiveDS.printSchema();

		///// people
		List<Person> people = Arrays.asList(
			new Person("김아무개", 23),
			new Person("홍길동", 25),
			new Person("김철수", 27));
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> personDS = spark.createDataset(people, personEncoder);

		//personDS.show();
		//personDS.printSchema();

		//타입연산
		//		personDS.map(new MapFunction<Person, Person>() {
		//			@Override
		//			public Person call(Person person) throws Exception {
		//				person.setAge(person.getAge() + 1);
		//				return person;
		//			}
		//		}, personEncoder).show();

		//비타입연산
		//personDS.select(col("name"), col("age").plus(30)).show();

		//SQL로 보기
		//personDS.createOrReplaceTempView("people");
		//spark.sql("SELECT * FROM people").show();

		spark.stop();
	}
}
