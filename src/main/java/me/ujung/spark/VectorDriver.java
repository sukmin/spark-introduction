package me.ujung.spark;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

import scala.Tuple2;

/**
 * @author sukmin.kwon
 * @since 2017-03-16
 */
public class VectorDriver {

	public static void main(String[] args) {

		/*
		벡터는 더블타입의 컬렉션으로, 0부터 시작하는 인덱스를 가지고 있음.
		 */

		Vector v1 = Vectors.dense(0.1, 0.0, 0.2, 0.3);
		Vector v2 = Vectors.dense(new double[] {0.1, 0.0, 0.2, 0.3});
		Vector v3 = Vectors.sparse(4,
			Arrays.asList(
				new Tuple2<>(0, 0.1),
				new Tuple2<>(2, 0.2),
				new Tuple2<>(3, 0.3)
			));
		Vector v4 = Vectors.sparse(4, new int[] {0, 2, 3}, new double[] {0.1, 0.2, 0.3});

		System.out.println(Arrays.stream(v1.toArray())
			.mapToObj(String::valueOf).collect(Collectors.joining(", "))
		);

		System.out.println(Arrays.stream(v2.toArray())
			.mapToObj(String::valueOf).collect(Collectors.joining(", "))
		);

		System.out.println(Arrays.stream(v3.toArray())
			.mapToObj(String::valueOf).collect(Collectors.joining(", "))
		);

		System.out.println(Arrays.stream(v4.toArray())
			.mapToObj(String::valueOf).collect(Collectors.joining(", "))
		);

	}

}
