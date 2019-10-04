package com.chen.guo

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object PrintArgs extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  LogManager.getRootLogger.setLevel(Level.DEBUG)
  LogManager.getLogger("log4j.logger.org.apache.hadoop.fs").setLevel(Level.DEBUG)

  for (arg <- args) {
    logger.info(s"arg: $arg")
  }

  val ss: SparkSession =
    SparkSession.builder()
      .appName("word count")
      //.enableHiveSupport()
      .getOrCreate()

  val sleep: Integer = Integer.valueOf(args(0))
  println(s"Will sleep for $sleep seconds")
  Thread.sleep(1000 * sleep)
}
