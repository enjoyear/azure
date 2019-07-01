package com.chen.guo

import org.apache.spark.sql.SparkSession

object SparkWordCount extends App {
  val ss: SparkSession =
    SparkSession.builder()
      .master("local")
      .appName("test")
      //.enableHiveSupport()
      .getOrCreate()

  val textFile = ss.sparkContext.textFile("hdfs://...")
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.saveAsTextFile("hdfs://...")
}
