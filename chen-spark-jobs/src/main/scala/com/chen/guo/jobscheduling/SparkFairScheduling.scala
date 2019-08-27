package com.chen.guo.jobscheduling

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark-shell --spark-version 2.3.0 --master yarn --deploy-mode client \
  * --conf spark.hive.support=true --conf spark.dynamicAllocation.executorIdleTimeout=3600 \
  * --conf spark.scheduler.mode=FAIR \
  * --conf spark.scheduler.allocation.file=/export/home/chguo/fairscheduler.xml
  *
  * WARN scheduler.FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO order.
  * To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a file that contains the configuration.
  */
object SparkFairScheduling extends App {
  val spark: SparkSession =
    SparkSession.builder()
      .appName("word count")
      //.enableHiveSupport()
      .getOrCreate()
  val sc = spark.sparkContext

  //  spark.conf.set("spark.scheduler.allocation.file", "/export/home/chguo/fairscheduler.xml")
  //  spark.conf.get("spark.scheduler.allocation.file")
  //  spark.conf.get("spark.scheduler.mode")

  val threadId: AtomicInteger = new AtomicInteger(0)
  val threadPool = Executors.newFixedThreadPool(2, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      sc.setLocalProperty("spark.scheduler.pool", "sharedPool")
      new Thread(r, s"Thread-${threadId.incrementAndGet()}")
    }
  })

  threadPool.submit(new Runnable {
    override def run(): Unit = {
      println(s"Submitting dim_position job in thread ${Thread.currentThread().getName}")
      val dimPosition: DataFrame = spark.table("prod_foundation_tables.dim_position")
      dimPosition.show
      dimPosition.limit(20).write.mode("overwrite").saveAsTable("u_chguo.dim_position")
    }
  })

  threadPool.submit(new Runnable {
    override def run(): Unit = {
      println(s"Submitting dim_member_std_geo job in thread ${Thread.currentThread().getName}")
      val dimMemberGeo: DataFrame = spark.table("prod_foundation_tables.dim_member_std_geo")
      dimMemberGeo.show
      dimMemberGeo.limit(100).write.mode("overwrite").saveAsTable("u_chguo.dim_member_std_geo")
    }
  })

  threadPool.shutdown()
}
