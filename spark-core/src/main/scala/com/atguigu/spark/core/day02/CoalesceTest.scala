package com.atguigu.spark.core.day02

import org.apache.spark.{SparkConf, SparkContext}

object CoalesceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(11,20,30,41,50)
    val rdd1 = sc.parallelize(list1,5)
    //val rdd2 = rdd1.coalesce(3,true)
    //rdd2.collect()
    val rdd2 = rdd1.repartition(6)
    //rdd2.collect.foreach(println)
    println("---------------")
    println(rdd1.getNumPartitions)
    println(rdd2.getNumPartitions)
    sc.stop()
  }
}
