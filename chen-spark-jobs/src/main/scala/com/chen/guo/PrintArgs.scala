package com.chen.guo

import java.util.function.Consumer

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
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
  val sc: SparkContext = ss.sparkContext

  val sleep: Integer = Integer.valueOf(args(0))
  println(s"Will sleep for $sleep seconds")
  Thread.sleep(1000 * sleep)

  ss.conf.getAll.foreach(x => logger.info(s"${x._1} -> ${x._2}"))
  sc.hadoopConfiguration.forEach(new Consumer[java.util.Map.Entry[String, String]] {
    override def accept(kvp: java.util.Map.Entry[String, String]): Unit = {
      logger.info(s"${kvp.getKey} => ${kvp.getValue}")
    }
  })

  val path = s"adl://adl0linkedin.azuredatalakestore.net/economic-graph/data/eg/linkedin-eg.txt"
  logger.info(s"Print out EG data at: $path")
  val raw = sc.textFile(path)
  raw.collect().foreach(println)
}
