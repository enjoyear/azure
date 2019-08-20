package com.chen.guo

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkWordCountForSupport extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  for (arg <- args) {
    logger.info(s"arg: $arg")
  }

  val ids = Map(
    "customer1" -> "id for customer1",
    "customer2" -> "id for customer2")

  val secrets = Map(
    "customer1" -> "secret for customer1",
    "customer2" -> "secret for customer1")

  val clientName = args(0)
  val srcData = args(1)
  val dest = args(2)

  val clientId = ids(clientName)
  val clientSecret = secrets(clientName)
  logger.info(s"Got id $clientId secret $clientSecret for $clientName")

  val spark: SparkSession =
    SparkSession.builder()
      .appName(s"WC-example-$clientName")
      .config("spark.driver.allowMultipleContexts", "true")
      .config("spark.hadoop.fs.azure.account.auth.type", "OAuth")
      .config("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      .config("spark.hadoop.fs.azure.account.oauth2.client.id", clientId)
      .config("spark.hadoop.fs.azure.account.oauth2.client.secret", clientSecret)
      .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint", "<endpoint here>")
      .getOrCreate()

  val sc = spark.sparkContext
  var src: RDD[String] = sc.textFile(srcData)
  val outputDir = new Path(dest, System.currentTimeMillis.toString)
  val counts: RDD[(String, Int)] = src.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.coalesce(1).saveAsTextFile(outputDir.toString)
}
