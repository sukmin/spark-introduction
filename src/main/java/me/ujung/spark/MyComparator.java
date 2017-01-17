package me.ujung.spark;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

/**
 * @author sukmin.kwon
 * @since 2017-01-17
 */
public class MyComparator implements Comparator<Tuple2<String, Long>>, Serializable {
	@Override
	public int compare(Tuple2<String, Long> wordAndCount1, Tuple2<String, Long> wordAndCount2) {

		long count1 = wordAndCount1._2();
		long count2 = wordAndCount2._2();

		if (count1 == count2) {
			return 0;
		}

		if (count1 < count2) {
			return -1;
		}

		return 1;
	}
}
