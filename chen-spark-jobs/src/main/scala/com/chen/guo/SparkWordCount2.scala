package com.chen.guo

import java.text.SimpleDateFormat

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWordCount2 extends App {
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
    println(s"arg: $arg")
  }
  val storageAccountConnectionString = args(args.length - 2)

  val cosmos = Map(
    "customer1" -> "6358f0cd-ce12-4e89-be99-66b16637880e",
    "customer2" -> "a75cef49-07f3-4028-bd1b-38731cf1ff4f")

  val clientNames = args.last
  private val clientNamesArray: Array[String] = clientNames.split(",")
  clientNamesArray.foreach(cn => {
    val clientName = cn.toLowerCase()
    val clientId = cosmos(clientName)
    val clientSecret = CredentialsFileProvider.getSecretFromSA(storageAccountConnectionString, clientName + "-secret")
    println(s"Got id $clientId secret $clientSecret for $clientName")

    val ss: SparkSession =
      SparkSession.builder()
        .appName(s"WC-example") //a spark application processing multiple customers' data
        .config("spark.yarn.maxAppAttempts", 1)
        .config("spark.submit.deployMode", "cluster")
        .config("spark.hadoop.fs.azure.account.auth.type", "OAuth")
        .config("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        .config("spark.hadoop.fs.azure.account.oauth2.client.id", clientId)
        .config("spark.hadoop.fs.azure.account.oauth2.client.secret", clientSecret)
        .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token")
        .getOrCreate()

    if (clientName != clientNamesArray(0)) {
      //session's conf v.s. sc's conf?
      println(s"Changing settings")
      ss.conf.set("spark.hadoop.fs.azure.account.auth.type", "OAuth")
      ss.conf.set("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      ss.conf.set("spark.hadoop.fs.azure.account.oauth2.client.id", clientId)
      ss.conf.set("spark.hadoop.fs.azure.account.oauth2.client.secret", clientSecret)
      ss.conf.set("spark.hadoop.fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/2445f142-5ffc-43aa-b7d2-fb14d30c8bd3/oauth2/token")
    }

    println(s"Printing configurations for $clientName")
    ss.conf.getAll.foreach(x => if (x._1.startsWith("spark.hadoop")) println(s"${x._1} -> ${x._2}"))
    println(s"spark.master -> ${ss.conf.get("spark.master")}")

    val sc = ss.sparkContext
    var inputs: RDD[String] = sc.emptyRDD[String]
    val egDataFullPath = args(0)
    println(s"In application ${sc.applicationId}: Get EG data path $egDataFullPath")
    inputs.union(sc.textFile(egDataFullPath))

    for (inputPathPart <- args.tail.dropRight(3)) {
      val path = s"abfss://$clientName@$inputPathPart"
      println(s"Get Raw data full path: $path")
      inputs = inputs.union(sc.textFile(path))
    }

    val dest = s"abfss://$clientName@${args(args.length - 3)}"
    val outputDir = new Path(dest, System.currentTimeMillis.toString)
    println(s"${getTime()} Will save output to: $outputDir")

    val counts: RDD[(String, Int)] = inputs.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println(s"${getTime()} Got counts")
    counts.coalesce(1).saveAsTextFile(outputDir.toString)
    println(s"${getTime()} Done for $clientName")
  })

  private def getTime(): String = {
    val dateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSZ")
    dateTime.format(System.currentTimeMillis())
  }
}
