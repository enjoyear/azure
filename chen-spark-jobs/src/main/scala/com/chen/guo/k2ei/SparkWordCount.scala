package com.chen.guo.k2ei

import com.chen.guo.auth.ICredentialProvider
import com.chen.guo.auth.security.KeyVaultADALAuthenticator
import com.microsoft.aad.adal4j.ClientCredential
import com.microsoft.azure.keyvault.KeyVaultClient
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkWordCount extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  for (arg <- args) {
    logger.info(s"arg: $arg")
  }

  val ss: SparkSession =
    SparkSession.builder()
      .appName("word count")
      .getOrCreate()

  val sc = ss.sparkContext

  var inputs: RDD[String] = sc.emptyRDD[String]
  logger.info(s"Creating RDD for: ${args(0)}")
  inputs = inputs.union(sc.textFile(args(0)))
  inputs.collect().foreach(x => logger.info(x))

  val counts: RDD[(String, Int)] = inputs.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  val dest = args(1)
  val outputDir = new Path(dest, System.currentTimeMillis.toString)
  logger.info(s"Saving output to: $outputDir")

  counts.coalesce(1).saveAsTextFile(outputDir.toString)
  println("Done")

  val vaultURL = "https://cdp-key-vault.vault.azure.net/"

  val credentials = new ICredentialProvider() {
    override def getClientCredential(clientName: String): ClientCredential = {
      val clientId = "48292e29-b7f5-49c1-bc1a-f29be8b362c8"
      val clientSecret = sc.hadoopConfiguration.get("spark.k2.spi-cdppoc-ei.secret")
      new ClientCredential(clientId, clientSecret)
    }

    override def getADId: String = "658728e7-1632-412a-9815-fe53f53ec58b"

    override def getSubscriptionId: String = "c3b5358d-d824-450c-badc-734462b9bbf1"
  }

//  val kvClient = new KeyVaultClient(KeyVaultADALAuthenticator.createCredentials(credentials, "spi-cdppoc-ei"))
//  val secret = kvClient.getSecret(vaultURL, "dummy-key")
//  logger.info("Fetched secret for dummy-key: " + secret.value)
}
