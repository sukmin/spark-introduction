package me.ujung.spark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-03-16
 */
public class PipelineDriver {

	public static void main(String[] args) throws IOException {
		SparkSession spark = SparkSession.builder()
			.appName("PipelineDriver")
			.master("local[*]")
			.getOrCreate();
		new JavaSparkContext(spark.sparkContext()).setLogLevel("ERROR");

		// 키와 체중, 나이로 성별을 알아낸다고 가정. label이 1이면 남, 0이면 여
		List<Person> trainingPeople = Arrays.asList(
			new Person(161.0, 69.87, 29, 1.0),
			new Person(176.78, 74.35, 34, 1.0),
			new Person(159.23, 58.32, 29, 0.0)
		);

		List<Person> testPeople = Arrays.asList(
			new Person(160.0, 51.2, 20, 0.0),
			new Person(190.4, 100.87, 61, 0.0),
			new Person(169.4, 69.87, 42, 0.0),
			new Person(170.9, 80.3, 25, 0.0),
			new Person(185.1, 85.0, 37, 0.0),
			new Person(161.6, 61.2, 28, 0.0)
		);

		Dataset<Row> training = spark.createDataFrame(trainingPeople, Person.class);
		Dataset<Row> test = spark.createDataFrame(testPeople, Person.class);
		training.cache();

		training.show(false);

		// 피쳐로 생각하는 3개의 컬럼값을 가진 벡터를 준비
		// assembler가 피쳐를 뽑아준다.
		VectorAssembler assembler = new VectorAssembler();
		assembler.setInputCols(new String[]{"height","weight","age"});
		assembler.setOutputCol("features");

		Dataset<Row> assembledTraining = assembler.transform(training);
		assembledTraining.show(false);

		//모델 생성 알고리즘 (로지스틱 회귀 평가자 : 대표적인 이진 분류 알고리즘)
		LogisticRegression lr = new LogisticRegression();
		lr.setMaxIter(10)
			.setRegParam(0.01)
			.setLabelCol("label")
			.setFeaturesCol("features");

		//모델 생성(훈련)
		LogisticRegressionModel model = lr.fit(assembledTraining);

		//예측값 생성
		model.transform(assembledTraining).show();

		// 테스트데이터 동작
		Dataset<Row> testTraining = assembler.transform(test);
		model.transform(testTraining).show();

		//파이프라인
		Pipeline pipeline = new Pipeline();
		pipeline.setStages(new PipelineStage[]{assembler, lr});

		//파이프라인 모델 생성
		PipelineModel pipelineModel = pipeline.fit(training);

		//파이프라인 모델을 이용한 예측값 생성
		//pipelineModel.transform(training).show();

		//모델 저장
		String output1 = "/Users/naver/git/spark-introduction/result/output1";
		String output2 = "/Users/naver/git/spark-introduction/result/output2";
		//model.write().overwrite().save(output1);
		//pipelineModel.write().overwrite().save(output2);

		//모델 불러오기
		//LogisticRegressionModel loadModel = LogisticRegressionModel.load(output1);
		//PipelineModel loadPipelineModel = PipelineModel.load(output2);


		spark.stop();
	}
}
