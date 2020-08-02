package com.atguigu.spark.core.day04

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("PartitionTest")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e"), (60, "f")))
    val rdd2 = rdd1.mapPartitionsWithIndex((index,it) => it.map(x => (index,x._1 + ":" + x._2)))
    println(rdd2.collect.mkString(","))
    val rdd3 = rdd1.partitionBy(new HashPartitioner(5))
    val rdd4 = rdd3.mapPartitionsWithIndex((index,it) => it.map(x => (index,x._1 + ":" + x._2)))
    println(rdd4.collect.mkString(","))
    sc.stop()
  }
}
