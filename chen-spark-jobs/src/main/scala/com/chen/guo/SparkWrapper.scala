package com.chen.guo

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}


object SparkWrapper extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  for (arg <- args) {
    println(s"arg: $arg")
  }

  val ss: SparkSession =
    SparkSession.builder()
      .appName("sparkWrapper")
      //.enableHiveSupport()
      .getOrCreate()

  println("Starting actual Spark job")
  new SparkWordCount(ss, args).run()
  println("Internal job completed")
}
