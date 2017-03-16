package me.ujung.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author sukmin.kwon
 * @since 2017-03-17
 */
public class LabeledPointDriver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
			.appName("SimpleDriver")
			.master("local[*]")
			.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		/**
		 * 레이블을 사용하는 경우를 위한 벡터
		 * 벡터와 마찬가지로 더블값만 할당가능
		 */

		Vector v1 = Vectors.dense(0.1, 0.0, 0.2, 0.3);
		LabeledPoint v5 = new LabeledPoint(1.0, v1);
		System.out.println("label:" + v5.label() + ", features:" + v5.features());

		String path = "/Users/naver/git/spark-introduction/sample001.txt";
		RDD<org.apache.spark.mllib.regression.LabeledPoint> v6 = MLUtils.loadLibSVMFile(sc.sc(), path);

		org.apache.spark.mllib.regression.LabeledPoint lp1 = v6.first();
		System.out.println("label:" + lp1.label() + ", features:" + lp1.features());

		spark.stop();
	}
}
