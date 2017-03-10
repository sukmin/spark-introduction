package me.ujung.spark;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-03-10
 */
public class ReadWriteDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.appName("ReadWriteDriver")
			.config("spark.sql.warehouse.dir","/Users/naver/git/spark-introduction/spark-warehouse")
			.master("local[*]")
			.getOrCreate();

		///// people
		List<Person> people = Arrays.asList(
			new Person("김아무개", 23),
			new Person("홍길동", 25),
			new Person("김철수", 27));
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> personDS = spark.createDataset(people, personEncoder);

		String jsonSaveDir = "/Users/naver/git/spark-introduction/result/json";

		personDS.write().format("json").mode(SaveMode.Overwrite).save(jsonSaveDir);

		Dataset<Row> jsonDF = spark.read().format("json").load(jsonSaveDir);
		jsonDF.show();

		spark.stop();
	}
}
