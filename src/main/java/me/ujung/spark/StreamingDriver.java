package me.ujung.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.gson.Gson;
import scala.Tuple2;

/**
 * @author sukmin.kwon
 * @since 2017-02-21
 */
public class StreamingDriver {

	public static void main(String[] args) throws InterruptedException {

		String checkPointPath = "/Users/naver/spark-2.0.2-bin-hadoop2.7/result/checkpoint";

		//소켓을 입력소스로 사용하고 로컬머신에서만 실행한다면 반드시 쓰레드는 2개이상으로 해주자. 쓰레드 1개일 경우 소켓 데이터를 정상적으로 읽어오지 못하는 문제 발생.
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TestStreaming");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		jssc.checkpoint(checkPointPath);
		jssc.sparkContext().setLogLevel("ERROR");


		/*
		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkPointPath, new Function0<JavaStreamingContext>() {
			@Override
			public JavaStreamingContext call() throws Exception {
				SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TestStreaming");
				JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

				String checkPointPath = "hdfs://dev-osdev-hadoop-001.ncl:9010/sukmin/checkpoint";
				jssc.checkpoint(checkPointPath);

				//TODO 여기서 모든 처리 로직을 실행

				return jssc;
			}
		});
		*/

		String host = "localhost";
		int port = 33334;

		JavaReceiverInputDStream<String> dStream = jssc.socketTextStream(host, port);

		JavaDStream<TestModel> testDStream = dStream.mapPartitions(new FlatMapFunction<Iterator<String>, TestModel>() {

			@Override
			public Iterator<TestModel> call(Iterator<String> stringIterator) throws Exception {

				Gson gson = new Gson();
				List<TestModel> testModels = new ArrayList<>();

				while (stringIterator.hasNext()) {
					String json = stringIterator.next();
					TestModel testModel = gson.fromJson(json, TestModel.class);
					testModels.add(testModel);
				}

				return testModels.iterator();
			}
		});

		//스코어가 90점 이상인 사람들 출력
		JavaDStream<TestModel> testDStreamWithOverScore90DStream = testDStream.filter(new Function<TestModel, Boolean>() {
			@Override
			public Boolean call(TestModel testModel) throws Exception {
				return testModel.getScore() >= 90;
			}
		});
		testDStreamWithOverScore90DStream.print();

		// 모든 스트림에서 상태를 가지고 싶은 경우. 이름마다 총 점수를 알고싶다.
		JavaPairDStream<String, Integer> userTotalScoreDStream = testDStream.mapToPair(new PairFunction<TestModel, String, TestModel>() {
			@Override
			public Tuple2<String, TestModel> call(TestModel testModel) throws Exception {
				return new Tuple2<>(testModel.getName(), testModel);
			}
		}).updateStateByKey(new Function2<List<TestModel>, Optional<Integer>, Optional<Integer>>() {
			@Override
			public Optional<Integer> call(List<TestModel> testModels, Optional<Integer> optional) throws Exception {
				int sum = optional.orElse(0);
				for (TestModel testModel : testModels) {
					sum = sum + testModel.getScore();
				}
				return Optional.of(sum);
			}
		});
		userTotalScoreDStream.print();

		// 1분동안 사람들의 이름당 나온 수. 30초마다 수행
		testDStream.mapToPair(new PairFunction<TestModel, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(TestModel testModel) throws Exception {
				return new Tuple2<>(testModel.getName(), 1);
			}
		}).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer + integer2;
			}
		}, Durations.seconds(60), Durations.seconds(30))
			.print();

		jssc.start();
		jssc.awaitTermination();
	}
}
