package com.chen.guo

import java.util
import java.util.Map
import java.util.function.Consumer

import com.chen.guo.auth.CredentialsFileProvider
import com.chen.guo.command.YarnCommandLineParser
import com.chen.guo.db.fakedCosmos
import com.chen.guo.fun.WebsiteReader
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory, MDC}

import scala.collection.JavaConverters._

object WordCountGen1MultipleSPs extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  for (arg <- args) {
    logger.info(s"arg: $arg")
  }

  if (!args(5).trim.isEmpty) {
    logger.info(s"Reading ${args(5).trim}")
    WebsiteReader.readWebsite(args(5).trim)
  }

  val pipelineRunId = args(4)
  MDC.put("pipeline_runid", pipelineRunId)
  val yarnAMCommand = System.getProperty("sun.java.command")
  logger.info(yarnAMCommand)
  MDC.put("activity_runid", YarnCommandLineParser.getActivityId(yarnAMCommand))

  LogManager.getRootLogger.setLevel(Level.DEBUG)
  LogManager.getLogger("log4j.logger.org.apache.hadoop.fs").setLevel(Level.DEBUG)

  for (prop <- System.getProperties.entrySet().asScala) {
    logger.info(s"Sys Prop: ${prop.getKey} -> ${prop.getValue}")
  }

  for (prop <- System.getenv().asScala) {
    logger.info(s"Sys Env: ${prop._1} -> ${prop._2}")
  }

  val egDataFullPath = args(0)
  val customerInput = args(1)
  val output = args(2)
  val storageAccountConnectionString = args(3)
  val clientName = "customer1"


  private val builder = SparkSession.builder()
    .appName(s"WC-$clientName-${System.currentTimeMillis()}")
    .config("spark.hadoop.fs.azure.account.auth.type", "OAuth")
    .config("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")


  val client1Id = fakedCosmos.getId(fakedCosmos.customer1)
  val client1Secret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, fakedCosmos.customer1 + "-secret")
  //For ADLv2
  builder
    //.config("spark.hadoop.fs.azure.account.oauth2.client.id", client1Id)
    //.config("spark.hadoop.fs.azure.account.oauth2.client.secret", client1Secret)
    .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token")

  val client2Id = fakedCosmos.getId(fakedCosmos.customer2)
  val client2Secret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, fakedCosmos.customer2 + "-secret")
  /**
    * https://hadoop.apache.org/docs/r2.8.0/hadoop-azure-datalake/index.html
    * https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-databricks-account
    *
    * org.apache.hadoop.fs.adl.AdlConfKeys
    *
    * org.apache.hadoop.fs.adl.AdlFileSystem
    * private AccessTokenProvider getAccessTokenProvider(config: Configuration)
    */
  //For ADLv1
  builder
    .config("spark.hadoop.fs.adl.oauth2.access.token.provider.type", "ClientCredential")
    //.config("spark.hadoop.fs.adl.oauth2.client.id", client2Id)
    //.config("spark.hadoop.fs.adl.oauth2.credential", client2Secret)
    .config("spark.hadoop.fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token")

  val spark: SparkSession = builder.getOrCreate()

  logger.info(s"-----------  Printing Spark configurations  -----------")
  spark.conf.getAll.foreach(x => logger.info(s"${x._1} -> ${x._2}"))

  val sc = spark.sparkContext
  logger.info(s"===========  Printing Hadoop configurations  ===========")
  sc.hadoopConfiguration.forEach(new Consumer[Map.Entry[String, String]] {
    override def accept(kvp: util.Map.Entry[String, String]): Unit = {
      logger.info(s"${kvp.getKey} => ${kvp.getValue}")
    }
  })

  var inputs: RDD[String] = sc.emptyRDD[String]
  logger.info(s"In application ${sc.applicationId}: Get EG data path $egDataFullPath")
  val eg: RDD[String] = sc.textFile(egDataFullPath)
  logger.info(s"Input Data - EG: ${eg.collect().mkString("\n")}")
  inputs = inputs.union(eg)

  val path = s"abfss://$clientName@$customerInput"
  logger.info(s"Get Raw data full path: $path")
  val raw = sc.textFile(path)
  logger.info(s"Input Data - Raw: ${raw.collect().mkString("\n")}")
  inputs = inputs.union(raw)

  logger.info(s"Input Data - All: ${inputs.collect().mkString("\n")}")

  val outputDir = new Path(s"abfss://$clientName@$output", System.currentTimeMillis.toString)
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
