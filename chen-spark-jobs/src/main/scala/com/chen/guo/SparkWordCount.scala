package com.chen.guo

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkWordCount extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

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
  for (arg <- args.dropRight(2)) {
    println(s"Got input: $arg")
    inputs = inputs.union(sc.textFile(arg))
  }

  logger.info("Got customer id: " + sc.hadoopConfiguration.get("fs.azure.account.oauth2.client.id", "unknown"))
  logger.info("Got customer secret: " + sc.hadoopConfiguration.get("fs.azure.account.oauth2.client.secret", "unknown"))

  val counts: RDD[(String, Int)] = inputs.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  val dest = args(args.length - 2)
  val outputDir = new Path(dest, System.currentTimeMillis.toString)
  println(s"Saving output to: $outputDir")

  val sleep: Integer = Integer.valueOf(args(args.length - 1))
  println(s"Will sleep for ${sleep} seconds")
  Thread.sleep(1000 * sleep)
  counts.coalesce(1).saveAsTextFile(outputDir.toString)
  println("Done")
}
