#!/usr/bin/env bash
SPARK_HOME=/Users/naver/spark
SPARK_JAR_NAME=/Users/naver/git/spark-introduction/target/spark-introduction-1.0-jar-with-dependencies.jar
MAIN_CLASS_NAME=me.ujung.spark.App

RUN_COMMAND="${SPARK_HOME}/bin/spark-submit --class ${MAIN_CLASS_NAME} --master local[*] ${SPARK_JAR_NAME}"

echo "mvn build"
mvn clean package

echo "${RUN_COMMAND}"
sleep 2
${RUN_COMMAND}
