package com.chen.guo.fun

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkFun extends App {

  class MyPartitioner(val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      key match {
        case x: Int => x % numPartitions
        case _ => throw new RuntimeException("unexpected type")
      }
    }
  }

  val spark: SparkSession =
    SparkSession.builder()
      .appName("word count")
      //.enableHiveSupport()
      .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val part1 = Range(0, 5).map(i => (1, "abc1", i))
  val part2 = Range(0, 3).map(i => (2, "abc2", i))
  val part3 = Range(0, 2).map(i => (3, "abc3", i))
  val data: RDD[(Int, String, Int)] = sc.parallelize(part1 ++ part2 ++ part3)
  var rdd: RDD[(Int, String, Int)] = data
  /**
    * How to partition it using id%3??
    */
  //rdd = rdd.partitionBy(new MyPartitioner(3))
  val test = rdd.toDF("id", "name", "row_no")

  val test1 = test.repartition(3, test("id"))
  /**
    * Why it doesn't print??
    */
  test1.foreachPartition(iter => {
    println(s"${iter.toList.mkString(",")}")
  })
  /**
    * Why this only has two output file instead of three files???
    */
  test1.write.mode("overwrite").format("orc").saveAsTable("u_chguo.dim_member_std_geo2")

  test.repartition(10).write.mode("overwrite").format("orc").saveAsTable("u_chguo.dim_member_std_geo20")
}
