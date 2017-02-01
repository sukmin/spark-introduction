package me.ujung.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author sukmin.kwon
 * @since 2017-01-23
 */
public class RddUtils {

	public static void collectAndPrint(JavaRDD<?> rdd) {
		System.out.println(rdd.collect().toString());
	}

	public static void collectAndPrint(String name, JavaRDD<?> rdd) {
		System.out.println(name + " : " + rdd.collect().toString());
	}

	public static void collectAndPrint(String name, JavaPairRDD<?,?> rdd) {
		System.out.println(name + " : " + rdd.collect().toString());
	}

}
