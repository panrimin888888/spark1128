package com.atguigu.spark.core.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionByTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List("hello","hello","world","spark","hello")
    val rdd1 = sc.parallelize(list1,2).map((_,1))
    val rdd2 = rdd1.map({
      case (k,v) => (v,k)
    }).partitionBy(new HashPartitioner(3)).map({
      case (v,k) => (k,v)
    })
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
