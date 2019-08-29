package com.chen.guo

import com.chen.guo.auth.CredentialsFileProvider
import com.chen.guo.db.fakedCosmos
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkWordCount2Multiple extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  LogManager.getRootLogger.setLevel(Level.DEBUG)
  LogManager.getLogger("log4j.logger.org.apache.hadoop.fs").setLevel(Level.DEBUG)

  /**
    * 1st arg: full eg path
    * 2nd arg: partial customer2 raw data path
    * 3rd arg: partial conflated output path
    * 4th arg: connection string
    * 5th arg: customer2 name
    *
    * ERROR ApplicationMaster: User class threw exception:
    * org.apache.spark.SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243).
    * To ignore this error, set spark.driver.allowMultipleContexts = true.
    */
  for (arg <- args) {
    logger.info(s"arg: $arg")
  }
  val storageAccountConnectionString = args(args.length - 2)

  val clientNames = args.last
  val clientNamesArray: Array[String] = clientNames.split(",").map(x => x.toLowerCase())
  val clientName = clientNamesArray(0) //first client name will be the output fs name

  private val builder = SparkSession.builder()
    .appName(s"WC-$clientName-${System.currentTimeMillis()}")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.hadoop.fs.azure.account.auth.type", "OAuth")
    .config("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")


  val client1Id = fakedCosmos.getId(fakedCosmos.customer1)
  val client1Secret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, fakedCosmos.customer1 + "-secret")
  builder
    .config("spark.hadoop.fs.azure.account.oauth2.client.id", client1Id)
    .config("spark.hadoop.fs.azure.account.oauth2.client.secret", client1Secret)
    .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token")

  //  val client2Id = fakedCosmos.getId(fakedCosmos.customer2)
  //  val client2Secret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, fakedCosmos.customer2 + "-secret")
  //  builder
  //    .config("spark.hadoop.fs.azure.account.oauth2.client.id.adl0linkedin", client2Id)
  //    .config("spark.hadoop.fs.azure.account.oauth2.client.secret.adl0linkedin", client2Secret)

  val spark: SparkSession = builder.getOrCreate()

  logger.info(s"Printing configurations")
  logger.info(s"Got secret: ${spark.conf.get("k2.akv.accessor.secret", "unknown")}")
  logger.info(s"Got secret: ${spark.conf.get("spark.k2.akv.accessor.secret", "unknown")}")
  logger.info(s"Got secret: ${spark.conf.get("spark.hadoop.k2.akv.accessor.secret", "unknown")}")

  spark.conf.getAll.foreach(x => if (x._1.startsWith("spark.hadoop")) logger.info(s"${x._1} -> ${x._2}"))
  logger.info(s"spark.master -> ${spark.conf.get("spark.master")}")
  logger.info(s"spark.submit.deployMode -> ${spark.conf.get("spark.submit.deployMode")}")

  val sc = spark.sparkContext
  var inputs: RDD[String] = sc.emptyRDD[String]
  val egDataFullPath = args(0)
  logger.info(s"In application ${sc.applicationId}: Get EG data path $egDataFullPath")
  val eg: RDD[String] = sc.textFile(egDataFullPath)
  logger.info(s"Input Data - EG: ${eg.collect().mkString("\n")}")
  inputs = inputs.union(eg)

  for (inputPathPart <- args.tail.dropRight(3)) {
    val path = s"abfss://$clientName@$inputPathPart"
    logger.info(s"Get Raw data full path: $path")
    val raw = sc.textFile(path)
    logger.info(s"Input Data - Raw: ${raw.collect().mkString("\n")}")
    inputs = inputs.union(raw)
  }

  logger.info(s"Input Data - All: ${inputs.collect().mkString("\n")}")

  val dest = s"abfss://$clientName@${args(args.length - 3)}"
  val outputDir = new Path(dest, System.currentTimeMillis.toString)
  logger.info(s"Will save output to: $outputDir")

  val counts: RDD[(String, Int)] = inputs.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  logger.info(s"Got conflated counts: ${counts.collect().mkString("\n")}")
  counts.coalesce(1).saveAsTextFile(outputDir.toString)
  logger.info(s"Done for $clientName")
  spark.stop()
  logger.info(s"Stopped the context")
}
