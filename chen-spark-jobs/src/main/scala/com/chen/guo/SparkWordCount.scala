package com.chen.guo

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWordCount extends App {
  for (arg <- args) {
    println(s"arg: $arg")
  }

  val ss: SparkSession =
    SparkSession.builder()
      .appName("word count")
      //.enableHiveSupport()
      .getOrCreate()

  val sc = ss.sparkContext

  var inputs: RDD[String] = sc.emptyRDD[String]
  for (arg <- args.dropRight(1)) {
    println(s"Got input: $arg")
    inputs = inputs.union(sc.textFile(arg))
  }

  val counts: RDD[(String, Int)] = inputs.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  val dest = args(args.length - 1)
  val outputDir = new Path(dest, System.currentTimeMillis.toString)
  println(s"Saving output to: $outputDir")

  counts.coalesce(1).saveAsTextFile(outputDir.toString)
  println("Done")
}
