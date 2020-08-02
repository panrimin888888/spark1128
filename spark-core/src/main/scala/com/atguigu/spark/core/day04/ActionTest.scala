package com.atguigu.spark.core.day04

import org.apache.spark.{SparkConf, SparkContext}

object ActionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ActionTest")
    val sc = new SparkContext(conf)
    val list1 = List(32,20,31,40,1)
    val rdd1 = sc.parallelize(Array(("a", 10), ("a", 20), ("b", 100), ("c", 200)),2)
//    val l = rdd1.filter(x => {
//      println("filter" + x)
//      x > 30
//    }).map(x => {
//      println("map" + x)
//      x * x
//    }).count
    val result = rdd1.countByKey()
    println(result)
  }
}
