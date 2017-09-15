package me.ujung.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author sukmin.kwon
  * @since 2017-09-14
  */
object First {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val singers = List("소녀시대", "아이유", "서태지")
    val rdd = sc.parallelize(singers)

    println(rdd.count())

  }

}
