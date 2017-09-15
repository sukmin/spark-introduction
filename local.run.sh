#!/usr/bin/env bash
SPARK_HOME=/Users/naver/spark
SPARK_JAR_NAME=/Users/naver/git/spark-introduction/target/scala-2.11/spark-introduction-assembly-1.0.jar
MAIN_CLASS_NAME=me.ujung.spark.First

RUN_COMMAND="${SPARK_HOME}/bin/spark-submit --class ${MAIN_CLASS_NAME} --master local[*] ${SPARK_JAR_NAME}"

echo "SBT clean"
sbt clean

echo "SBT assembly"
sbt assembly

echo "${RUN_COMMAND}"
sleep 2
${RUN_COMMAND}
