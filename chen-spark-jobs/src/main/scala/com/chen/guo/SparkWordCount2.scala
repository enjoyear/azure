package com.chen.guo

import com.chen.guo.auth.CredentialsFileProvider
import com.chen.guo.db.fakedCosmos
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkWordCount2 extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  LogManager.getRootLogger.setLevel(Level.DEBUG)
  LogManager.getLogger("log4j.logger.org.apache.hadoop.fs").setLevel(Level.DEBUG)

  /**
    * 1st arg: full eg path
    * 2nd arg: partial customer raw data path
    * 3rd arg: partial conflated output path
    * 4th arg: connection string
    * 5th arg: customer name
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
  private val clientNamesArray: Array[String] = clientNames.split(",")
  clientNamesArray.foreach(cn => {
    val clientName = cn.toLowerCase()
    val clientId = fakedCosmos.getId(clientName)
    val clientSecret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, clientName + "-secret")
    logger.info(s"Got id $clientId secret $clientSecret for $clientName")

    val spark: SparkSession =
      SparkSession.builder()
        .appName(s"WC-$clientName-${System.currentTimeMillis()}") //a spark application processing multiple customers' data
        //.config("spark.yarn.maxAppAttempts", 1)
        //.config("spark.submit.deployMode", "cluster")
        //        .config("spark.hadoop.cloneConf", "true")
        //        .config("spark.hadoop.fs.abfss.impl.disable.cache", "true")
        //        .config("spark.hadoop.fs.abfs.impl.disable.cache", "true")
        .config("spark.driver.allowMultipleContexts", "true")
        .config("spark.hadoop.fs.azure.account.auth.type", "OAuth")
        .config("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        .config("spark.hadoop.fs.azure.account.oauth2.client.id", clientId)
        .config("spark.hadoop.fs.azure.account.oauth2.client.secret", clientSecret)
        .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token")
        .getOrCreate()

    logger.info(s"Printing configurations for $clientName")
    spark.conf.getAll.foreach(x => if (x._1.startsWith("spark.hadoop")) logger.info(s"${x._1} -> ${x._2}"))
    logger.info(s"spark.master -> ${spark.conf.get("spark.master")}")
    logger.info(s"spark.submit.deployMode -> ${spark.conf.get("spark.submit.deployMode")}")

    val sc = spark.sparkContext
    var inputs: RDD[String] = sc.emptyRDD[String]
    val egDataFullPath = args(0)
    logger.info(s"In application ${sc.applicationId}: Get EG data path $egDataFullPath")
    inputs = inputs.union(sc.textFile(egDataFullPath))

    for (inputPathPart <- args.tail.dropRight(3)) {
      val path = s"abfss://$clientName@$inputPathPart"
      logger.info(s"Get Raw data full path: $path")
      inputs = inputs.union(sc.textFile(path))
    }

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
  })
}
