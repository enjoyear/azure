package com.chen.guo

import java.io.StringReader

import com.chen.guo.security.KeyVaultADALAuthenticator
import com.microsoft.azure.keyvault.KeyVaultClient
import com.microsoft.azure.storage.CloudStorageAccount
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkWordCount extends App {
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
  for (arg <- args.dropRight(1)) {
    println(s"Creating RDD for: $arg")
    inputs = inputs.union(sc.textFile(arg))
  }

  val counts: RDD[(String, Int)] = inputs.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  val dest = args(args.length - 2)
  val outputDir = new Path(dest, System.currentTimeMillis.toString)
  println(s"Saving output to: $outputDir")

  counts.coalesce(1).saveAsTextFile(outputDir.toString)
  println("Done")

  val storageAccountConnectionString = args(args.length - 1)
  println(String.format("Storage Account Connection String: %s", storageAccountConnectionString))
  val storageAccount = CloudStorageAccount.parse(storageAccountConnectionString)
  val cloudBlobClient = storageAccount.createCloudBlobClient
  val container = cloudBlobClient.getContainerReference("demo-jars")
  val blockRef = container.getBlockBlobReference("properties/azure_credentials.properties")
  val s = blockRef.downloadText
  println("Blob Content: " + s)
  val credentials = new CredentialsFileProvider(new StringReader(s))
  val sp = "akv-reader"
  //this sp must be granted access in the KV's Access Policies
  val vaultURL = "https://chen-vault.vault.azure.net/"

  val kvClient = new KeyVaultClient(KeyVaultADALAuthenticator.createCredentials(credentials, sp))
  val secret = kvClient.getSecret(vaultURL, "sas-token")
  println("Fetched SAS token: " + secret.value)

}
